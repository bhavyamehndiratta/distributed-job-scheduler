# Design: Fault-Tolerant Distributed Job Scheduler

Status: draft, written before implementation. Sections marked "Filled in later" are completed from real runs, never estimated.

## 1. Problem

Clients need to submit jobs (run once at a given time, or on a cron schedule) and get a guarantee that each job's effect happens exactly once, even when scheduler nodes crash, restart, or are partitioned from each other. A single scheduler process is a single point of failure: if it dies, jobs stop firing, and if it is restarted naively, in-flight jobs may run twice or be lost.

This project builds a 3 to 5 node scheduler cluster that replicates every scheduling decision through the Raft consensus algorithm, implemented from scratch, and layers an exactly-once-effects protocol on top.

## 2. Requirements

### Functional
- Submit, query, cancel and list jobs through a client library and CLI.
- Job types: one-shot (fire at a time) and recurring (cron expression).
- Job lifecycle: SUBMITTED, SCHEDULED, ASSIGNED, RUNNING, SUCCEEDED, FAILED, with a bounded retry policy.
- Jobs are assigned to separate worker processes; a stuck or dead worker's job is reassigned after a timeout.
- Cluster membership can change while the cluster is serving traffic.

### Non-functional
- Safety over availability: with a majority of nodes up and connected, the cluster makes progress; without a majority, it stops accepting writes rather than risk divergence.
- No acknowledged job is ever lost.
- No job's effect is applied more than once.
- Every failure found in testing is reproducible from a single seed.

### Out of scope
- Multi-tenant authentication and authorization.
- Running arbitrary shell commands on workers. Jobs invoke registered handlers (see section 6.4). This keeps the effect of a job well defined, which is what the exactly-once checker needs.
- Cross-datacenter deployment (discussed in section 11).

## 3. Architecture overview

```
 client / CLI --gRPC--> leader <--gRPC--> followers (Raft)
                          |
                          v
                  replicated state machine
                  (job table, assignments, completed executions)
                          ^
                          |
 worker processes --gRPC--+  poll for work, heartbeat, report results
```

Each node runs the same code:
- **Raft module**: election, log replication, persistence, snapshots, membership, read path.
- **Scheduler state machine**: a deterministic function `apply(entry) -> new state` over the committed log.
- **Scheduler service**: on the leader only, turns time passing and worker reports into proposed log entries.
- **Transport**: an interface with two implementations: real gRPC, and a simulated network used by tests.

Workers are separate processes. They are stateless with respect to scheduling: everything they need to know (what to run, which fencing token to use) comes from the cluster.

### Key structural decision: event-driven nodes
A Raft node is written as a single-threaded state machine with no internal threads or calls to the system clock. It is driven from outside by exactly three kinds of input:
- `tick()`: logical time advanced by one unit,
- `receive(message)`: a message arrived,
- `propose(command)`: a client wants something replicated.

In production, a single-threaded executor calls these, and `tick()` is fired by a real timer. In tests, a discrete-event simulator calls them with a virtual clock and a seeded random number generator. Because the node never touches real time, threads or randomness except through injected interfaces, an entire multi-node run, including crashes and partitions, replays identically from its seed. This is the same idea used by deterministic simulation testing systems such as FoundationDB's, and it is what makes the chaos harness's failures debuggable.

Trade-off: the event-driven style is less natural to write than "one thread per RPC", and real deployments still need care at the boundary (the gRPC threads must hand work to the node's single thread). In exchange, correctness testing becomes reproducible, and there are no data races inside the consensus code.

## 4. Raft

### 4.1 Election
- Followers become candidates after an election timeout with no valid heartbeat.
- Timeouts are randomized per node and per election (for example, a base of 10 ticks plus a random 0 to 10 ticks) so that two nodes rarely time out together, which avoids repeated split votes.
- A node grants at most one vote per term, and only to a candidate whose log is at least as up to date as its own (higher last term, or equal last term and greater or equal last index). This rule is what guarantees a new leader holds every committed entry.
- **PreVote**: before incrementing its term, a candidate asks peers whether they would vote for it. A node that was partitioned away therefore does not return with an inflated term and force a healthy leader to step down. This is an extension from Ongaro's dissertation, included because the chaos harness creates exactly this situation constantly.
- **CheckQuorum**: a leader that has not heard from a majority within an election timeout steps down to follower. Without it, a leader cut off in a minority partition keeps believing it is leader and keeps accepting client requests it can never commit. Together with PreVote, it keeps a partitioned node from either disrupting the cluster or holding on to leadership it has lost.

### 4.2 Log replication and commit
- The leader sends AppendEntries with `prevLogIndex` and `prevLogTerm`. A follower rejects entries whose predecessor does not match, and the leader backs up until the logs agree (the log matching property). Rejections carry a conflict hint (the conflicting term and its first index) so the leader skips back a whole term at a time instead of one entry at a time.
- An entry is committed when it is stored on a majority **and** it belongs to the leader's current term. Entries from earlier terms are committed only indirectly, by committing a current-term entry after them. Counting replicas alone is not enough for earlier-term entries: Figure 8 of the Raft paper shows such an entry can be on a majority and still be overwritten.
- On election, a new leader immediately appends a no-op entry in its own term. This commits everything before it quickly, and it is also required by the membership and read-path designs below.

### 4.3 Persistence
- `currentTerm` and `votedFor` are written to a small metadata file using write-to-temp, fsync, then atomic rename, so a crash never leaves a half-written file.
- The log is an append-only file of length-prefixed records, each with a CRC32 checksum. On startup, a torn record at the tail (a crash in the middle of a write) is detected by the checksum and truncated. A bad checksum anywhere else is treated as corruption and the node refuses to start.
- A node persists state **before** it replies to the RPC that changed it (votes, appended entries). Replying first and then crashing would let the node vote twice in one term, or acknowledge entries it then forgets.
- Storage sits behind an interface. Tests use an in-memory implementation that can simulate a crash by discarding anything not yet "fsynced".

### 4.3.1 Group commit
Syncing the log after every single append made the first benchmark flood crawl at 3.2 submits per second, because every node thread sat in `FileChannel.force`. The log now supports deferred sync: appends and truncations are written without fsync and marked dirty, and the node driver processes a batch of queued tasks (up to 256) plus the tick, then calls `sync()` once and only then releases the outgoing messages (`BatchedTransport` holds them until the flush). Nothing leaves a node before the entries it depends on are durable, so the persistence rules in 4.3 are unchanged; only the number of fsyncs per entry drops. The simulated chaos runs use in-memory or per-append logs, so group commit is exercised by the real-file and gRPC tests and by the benchmark integrity checks, not by the seeded chaos sweeps.

### 4.4 Snapshots and log compaction
- When the log grows past a configured size, a node serializes the state machine to a snapshot file tagged with `lastIncludedIndex` and `lastIncludedTerm`, then discards log entries up to that index.
- A follower that is so far behind that the leader no longer has the entries it needs receives an InstallSnapshot RPC instead, sent in chunks, then continues with normal AppendEntries.
- Snapshot writes use the same temp-file-and-rename pattern as the metadata file.
- A snapshot contains everything needed to rebuild the node without the discarded log: the full scheduler state (jobs, runs, current fencing tokens, completed runs and idempotency-key records with their timestamps) and the latest cluster configuration as of `lastIncludedIndex`. The configuration must be included explicitly, because the configuration entry that defined it may have been compacted away.

### 4.5 Membership changes: single-server changes with learners
**Decision**: membership changes add or remove one voting node at a time. New nodes join first as non-voting learners and are promoted to voters once they have caught up.

**Why**: safety requires that any two majorities overlap, so that two leaders cannot be elected in the same term and a new leader always holds every committed entry. Switching the whole configuration at once breaks this: during the switch some nodes count votes with the old member list and some with the new one, and an old majority and a new majority can be disjoint. When the old and new configurations differ by exactly one node, every majority of one overlaps every majority of the other, so the guarantee holds through the change.

Details:
- A configuration change is a log entry. A node uses the latest configuration entry in its log as soon as it is appended, not when it commits.
- Only one configuration change may be uncommitted at a time.
- A leader may not start a configuration change until it has committed an entry from its own term (the no-op in 4.2). Ongaro published this fix after his dissertation: without it, single-server changes can lose a committed configuration change across a leader change.
- Learners receive the log but do not vote and are not counted for commit. Without this step, adding an empty node to a 3-node cluster would make a 4-node cluster that needs 3 acknowledgements while the new node is still copying the log, which hurts availability.
- **Promotion criterion.** A learner counts as caught up when its match index is within a configured number of entries of the leader's last log index (default 50) for a configured number of consecutive heartbeats (default 3). Only then does the leader propose the configuration change that makes it a voter.
- **Removing the leader itself.** If the change removes the current leader, the leader keeps leading while the change is uncommitted, replicating the entry but not counting its own vote toward the new configuration's majority. Once the entry commits, it steps down and stops sending heartbeats, and the remaining nodes elect a new leader.

**Alternative considered: joint consensus**, the original Raft paper's method, which moves through a transitional configuration requiring majorities of both the old and new sets. It can change several nodes in one step, but it has more states and more edge cases. A 3 to 5 node cluster never needs to change more than one node at a time, so the simpler design was chosen and the saved effort went into testing.

### 4.6 Read path: ReadIndex
**Decision**: status and list queries are served with ReadIndex instead of going through the log. Lease-based reads are not used.

How ReadIndex works:
1. The leader records its current commit index as the read index. (It must already have committed an entry in its current term, which the no-op guarantees.)
2. It confirms it is still leader by getting heartbeat acknowledgements from a majority. This rules out a newer leader having been elected elsewhere.
3. It waits until its state machine has applied up to the read index, then answers from local state.

This gives linearizable reads (a read sees every write that completed before it started) at the cost of one heartbeat round, without writing anything to the log or to disk.

**Alternative considered: leader leases.** A leader that recently heard from a majority assumes no other leader can exist for a lease period and answers reads with no network round at all. This is faster, but it is only safe if clocks on different machines run at nearly the same rate. A long garbage-collection pause, a paused VM, or clock drift can let an old leader serve a stale read while a new leader has already accepted writes. ReadIndex makes no assumption about clocks, which matches this project's priority of safety over latency. Leases would be the next optimization if read latency mattered more.

## 5. Scheduler state machine

The state machine is a pure, deterministic function of the committed log. It never reads the clock or generates random values. Any time it needs is carried inside the log entry, stamped by the leader that proposed it. This is required: every node applies the same entries and must reach the same state.

### 5.1 Commands (log entry payloads)
| Command | Effect |
|---|---|
| `SubmitJob(jobId, spec, idempotencyKey)` | Creates the job. A repeat with the same idempotency key returns the existing job instead of creating a second one. |
| `CancelJob(jobId)` | Marks the job cancelled; no further runs are scheduled. |
| `FireRun(jobId, scheduledTime, skippedCount)` | Creates a run for that scheduled time. The run id is `jobId + scheduledTime`. Each job stores the latest scheduled time it has fired, and a `FireRun` at or before that time is ignored, so duplicates are rejected without keeping every past run id forever. `skippedCount` is chosen by the leader (the number of missed fire times it is skipping under the misfire policy) and stored in the entry, so the state machine never reads a clock. |
| `AssignRun(runId, workerId, leaseTicks)` | Assigns the run to a worker. The run's fencing token is set to the log index of this entry. |
| `RunStarted(runId, token)` | Accepted only if `token` equals the run's current token. |
| `RunCompleted(runId, token, outcome)` | Accepted only if `token` equals the run's current token; records the run as finished. |
| `ExpireAssignment(runId, token)` | Leader-proposed when a worker's lease runs out; returns the run to the queue for a retry. |
| `ConfigChange(...)` | Membership change (section 4.5). |

### 5.2 Run lifecycle
A job is either ACTIVE or CANCELLED. The states below belong to a run, and a job whose only run has succeeded stays ACTIVE. SUBMITTED is the moment the job is accepted, before any run exists.

SUBMITTED (job accepted) then SCHEDULED (a run exists with a fire time) then ASSIGNED (given to a worker with a token) then RUNNING (worker confirmed start) then SUCCEEDED or FAILED. A failed or expired run goes back to SCHEDULED until it reaches its retry limit, after which it stays FAILED. Recurring jobs get a new run for each cron fire time.

### 5.3 Time
The leader's scheduler service checks, on each tick, which runs are due according to its local clock and proposes `FireRun` entries. If the leader changes, the new leader may propose a `FireRun` that the old leader already proposed; the deterministic run id makes the duplicate a no-op. Clock skew between nodes can shift when a run fires, but it cannot make a run fire twice.

**Misfire policy.** If a recurring job's fire times pass while no leader is available, for example during a long outage or while the cluster has lost its majority, the next leader fires **only the most recent missed time** and skips the older ones. Because `FireRun` rejects anything at or before a job's latest fired time, firing the most recent time also marks every earlier missed time as passed. The number of skipped fire times is recorded on the job so it is visible through `status`. One-shot jobs are never skipped: a one-shot job whose time passed during an outage fires once as soon as a leader is available.

Alternative considered: firing every missed run. After a long outage, a job that runs every minute would produce a burst of hundreds of runs at once, and most periodic jobs (reports, cleanups, syncs) only need the latest state anyway. Firing every missed run would be the right policy for jobs where each period's run matters on its own, such as per-hour billing, and would be added as a per-job option if that need came up.

## 6. Exactly-once effects

### 6.1 Why exactly-once delivery is impossible
A worker runs a job and then tells the cluster it finished. If the network drops that report, or the worker crashes right after the job's effect happened, the cluster cannot distinguish "the job never ran" from "the job ran but the report was lost". It must either retry (risking a second run) or not retry (risking zero runs). No protocol removes this ambiguity, because the cluster can only learn about the outside world through messages that can be lost. This is a form of the Two Generals problem.

### 6.2 What this system guarantees instead
**At-least-once execution plus deduplication of effects gives exactly-once effects.** Runs are retried until a completion is recorded, so nothing is lost. Every attempt carries an identity that lets the system that receives the effect recognize and reject duplicates and stale attempts.

### 6.3 The mechanisms
1. **Unique ids and idempotency keys.** Clients attach an idempotency key to submissions, so a client retry after a timeout does not create a second job. Keys are kept for a retention window (configurable, 24 hours by default), measured with the leader-stamped time in log entries so that every node expires the same keys. A retry that arrives after the window may create a new job; clients are expected to stop retrying well before then. Runs have deterministic ids (`jobId + scheduledTime`).
2. **Fencing tokens.** Every assignment gets a token equal to the Raft log index of its `AssignRun` entry. Committed log indexes are unique and strictly increasing, so a later assignment always has a larger token than an earlier one, across leader changes. A worker that was presumed dead, for example one that paused for a long time and then woke up, still holds an old token. When it reports back, the state machine rejects the report because the token is no longer current.
3. **Deduplicated, fenced effects.** Rejecting a stale report in the state machine is not enough on its own: the stale worker may already have applied its effect to the outside world. So every effect is written to an **effect store** together with its run id and token, and the store enforces two rules per run id:
   - **At most one effect per run id.** If a run already has an effect, the store does not apply another one and replies "already applied". The worker treats that reply as success and reports completion normally.
   - **No writes below the highest token seen.** The store remembers the highest token presented for each run and rejects any lower one as stale.

   The first rule is what guarantees exactly-once effects. A token check alone is not enough: if a paused worker holding token 5 wakes up and writes before the replacement worker holding token 9, the store has never seen 9 yet, so it accepts 5, and without deduplication it would then also accept 9, producing two effects. The second rule stops a stale worker from acting once a newer assignment has reached the store, which matters for effects made of more than one write. This is the general requirement for fencing: the protected resource itself must check the token. The effect store is also what the chaos checker reads to count effects.
4. **Replicated record of completions.** Completed run ids are part of the replicated state, so after any failover the new leader knows which runs are done and never reassigns them.

### 6.4 Job handlers
A job names a registered handler and its arguments. Built-in handlers cover what testing and benchmarking need, for example a handler that writes a record to the effect store and a handler that sleeps for a configured time. Arbitrary command execution is out of scope, because an effect that is not fenced cannot be made exactly-once.

## 7. Workers and assignment
- Workers register with the cluster and poll the leader for work, and the leader replies with an assignment and its token. Pull-based assignment means the leader never has to track which workers are reachable in order to push to them.
- A worker heartbeats while running. If the lease expires without a heartbeat, the leader proposes `ExpireAssignment`, and the run is retried with a new, larger token.
- Lease timers live only in the current leader's memory and are measured in its own ticks. When a new leader is elected, it restarts every outstanding lease from zero. It cannot trust how much time the old leader had already counted, and assuming the worst would expire healthy workers' runs too early. The cost is that a worker that died just before a failover is detected one lease period later than it otherwise would be.
- Workers hold no scheduling state, so a crashed worker is replaced by simply starting another one.

## 8. Client library and CLI
- Clients may contact any node. A follower replies with the current leader's address if it knows it, and the client retries there. If no leader is known, the client backs off with jitter and tries another node.
- Writes carry an idempotency key, so retrying after a timeout is always safe.
- CLI commands: `submit`, `status`, `cancel`, `list`.

## 9. Testing strategy

### 9.1 Unit tests
Election, vote rules, log matching and conflict resolution, the current-term commit rule, persistence and crash recovery (including torn writes), snapshotting and InstallSnapshot, membership changes, ReadIndex, idempotency keys and fencing checks.

### 9.2 Integration tests
A real 3-node cluster over gRPC on localhost, with real workers, exercised through the client library.

### 9.3 Chaos harness
Runs the full system (nodes, workers, clients and effect store) inside the deterministic simulator. Each scenario is generated from a seed and mixes a random workload with random faults:
- message drop, delay, duplication and reordering,
- network partitions, including partitions that isolate the leader and partitions that heal,
- node crash and restart, including the leader and in the middle of a job run, with unsynced writes discarded,
- worker crash and long worker pauses (to exercise fencing),
- membership changes during faults.

After each scenario, the faults are healed and the system is given time to settle, then a checker verifies:
1. **No lost jobs**: every job the cluster acknowledged exists and every due run eventually completed or reached FAILED after its retries.
2. **Exactly-once effects**: the effect store contains at most one effect per run, and exactly one for each run that succeeded.
3. **Log agreement**: all nodes' committed log prefixes are identical.
4. **Election safety**: no term ever had two leaders.

5. **Linearizable reads**: a probe client sends `GetJob` to every node that is up. A reply must show any job acknowledged before the request was sent, and must show its run as SUCCEEDED if it had already succeeded on some node. Seventy percent of reads target the three newest acknowledged jobs. In 40 percent of partitions the current leader is isolated together with the probe and its timers are stalled for 20 to 100 ticks (`SchedulerCluster.stallNode`), which is the stale-leader case ReadIndex exists for.

A failing seed is replayed exactly by passing it back to the harness. The sweeps in section 12 run 1,000 scenarios per harness with zero failures.

## 10. Bugs the harness caught
Each entry is a real failure found by the harness or by mutation testing while building this project. Seeds are not listed because the fixed code no longer fails; each bug now has a regression test.

1. **Stale worker wrote a second effect.** The first effect store only rejected lower fencing tokens. A paused worker holding token 5 could write before the retry holding token 9, giving two effects for one run. Fix: the store also dedupes per run id, so a second write for the same run is reported as already applied. Negative control: the naive store (`EffectStore(false)`) is caught duplicating effects in 59 of 60 seeds, and the protected store in none.
2. **PreVote could be ignored.** A deliberate mutation that made PreVote ignore a live leader survived the election tests. A direct PreVote unit test was added and now kills it.
3. **Run stuck in ASSIGNED.** A worker's one-shot Started message went to a follower and was never retried. Fix: the first valid worker heartbeat also proposes `RunStarted`.
4. **Retried submit reported DUPLICATE.** Over real gRPC a retried `Submit` came back DUPLICATE and the client treated it as a failure. Fix: the client reports DUPLICATE on a retried request for the same job id (and cancel) as ACCEPTED.
5. **Membership vote refusal deadlock.** The membership chaos harness found 7 of 200 seeds with no leader after healing. Nodes refused to vote for candidates missing from their own, stale, configuration, so two voters with different config views blocked each other. Fix: votes depend on the log and the leader lease only. A removed node cannot win because it never receives its own removal entry, so voters reject it on log freshness. The same harness also exposed a promote of a learner that had received nothing.
6. **ReadIndex mutant survived the chaos read check.** A mutant whose `readyReads` ignored the majority confirmation was killed by the unit tests but passed two versions of the chaos read check (200 seeds). A cut-off leader steps down through CheckQuorum within 10 to 20 ticks, about when a new leader commits, so the stale window almost never opened. Adding the timer-stall fault made the mutant fail 119 of 200 seeds with "stale read: acked job reported missing".
7. **Benchmark flood at 3.2 submits per second.** Thread dumps showed every node thread blocked in `FileChannel.force`. The first hypothesis, a slow fsync, was falsified by an isolated test (median 3.9 ms); the cause was one fsync per entry. Fixed with group commit (4.3.1).

## 11. Limitations and what would change at larger scale
- **Single Raft group.** All writes go through one leader, so throughput is capped by one machine and by fsync latency. At 100x load, jobs would be sharded across many Raft groups by job id, as systems such as CockroachDB and TiKV do, with a placement service assigning shards to nodes.
- **Pipelining.** Entries are already batched per AppendEntries (up to 64) and per fsync (4.3.1). AppendEntries are not pipelined: a larger system would send the next batch without waiting for each reply.
- **Across datacenters.** Commit latency becomes a cross-datacenter round trip. Common responses are placing a majority of replicas in nearby regions, using leader leases with bounded clock uncertainty for reads, and adding follower reads for queries that can tolerate slightly stale data.
- **Leader-driven timers.** Only the leader fires runs, so a large number of recurring jobs concentrates timer work on one node. Sharding addresses this as well.
- **Handler model.** Jobs are limited to registered handlers whose effects can be fenced.
- **Whole snapshots.** InstallSnapshot sends the snapshot in one message (the gRPC inbound limit is raised to 256 MB). A larger state would need chunked transfer.
- **Housekeeping scan.** The leader's housekeeping iterates all runs every 2 ticks, which is linear in the number of runs. An index of active runs would remove it.
- **Standalone worker effects.** `WorkerMain` uses an in-memory effect store. The exactly-once checks run against the shared store used by the harness and the benchmark.
- **Group commit coverage.** See 4.3.1: the seeded chaos runs do not exercise it.

## 12. Results
### 12.1 Chaos sweeps (simulated network, seeds 1 to 1000 per harness)
All four harnesses finished with 0 failed scenarios.

| Harness | Scenarios | Notable counts |
|---|---|---|
| Raft | 1000 | 85306 proposals, 83187 committed, 43883 crashes, 43801 partitions, 17685 elected terms, 28846 snapshots taken, 9011 installed |
| Scheduler | 1000 | 135814 jobs submitted, 134155 acked, 134155 succeeded with exactly one effect; 34981 node crashes, 30804 worker crashes, 40743 worker pauses, 36185 partitions; 1531033 read requests, 186427 answered |
| Scheduler with snapshots | 1000 | 134155 jobs succeeded, 123742 snapshots taken, 29833 installed |
| Membership | 1000 | 93477 proposals, 93175 committed, 50438 crashes, 38798 partitions; 9821 learners added, 5318 promotions, 8916 removals, 23495 config entries committed |

### 12.2 Benchmarks (real gRPC on localhost, files on disk)
Hardware: Apple M4, 10 cores, 16 GB RAM, macOS 15.5, Java 21.0.13, max heap 4096 MB. All nodes, 32 workers and the client threads run in one JVM, so the numbers describe a single machine and not a network. Tick 20 ms (election timeout 200 to 380 ms, heartbeat 40 ms). The job body lasts one tick. Command: `bin/djs bench --rate 20 --seconds 10 --jobs 2000 --trials 5`.

| Measure | 3 nodes | 5 nodes |
|---|---|---|
| Submit to execution start at 20 jobs/s, p50 | 74.6 ms | overloaded (6.9 s) |
| Submit to execution start at 20 jobs/s, p99 | 116.0 ms | overloaded (18.2 s) |
| Flood of 2000 jobs, committed submits per second | 52.6 | 49.0 |
| Flood of 2000 jobs, jobs per second end to end | 47.8 | 39.3 |
| Kill leader to new leader, median (5 trials) | 241.6 ms | 305.4 ms |
| Kill leader to first committed write, median | 915.3 ms | 926.1 ms |
| Acked jobs not executed / duplicate effects | 0 / 0 | 0 / 0 |

Notes:
- The first-write time includes the client's 15-tick resend interval.
- Throughput varies from run to run on the same machine. Three 3-node floods gave 47.8, 49.8 and 87.7 jobs/s end to end. The cause was not diagnosed.
- A thread-dump profile found no scan over all jobs; the time was in fsync and gRPC decoding.
- At 5 nodes the cluster saturated at 20 jobs/s in this single-JVM setup. Every run showed 0 lost and 0 duplicate effects at both sizes.
- A 50 jobs/s latency run overloaded the cluster (p50 4.9 s at 3 nodes) and is not reported as a latency figure.

## 13. Decision log
| Decision | Choice | Main alternative |
|---|---|---|
| Consensus | Raft, implemented from scratch | Existing library (excluded by design) |
| Node structure | Single-threaded, event-driven, deterministic | Thread per RPC |
| Membership | Single-server changes with learners | Joint consensus |
| Reads | ReadIndex | Leader leases |
| Fencing token | Log index of the `AssignRun` entry | Separate counter or term-based token |
| Assignment | Workers pull from the leader | Leader pushes to workers |
| Language and RPC | Java 21, gRPC with protobuf | |
| Log durability | Group commit: one fsync per batch, messages held until it completes | fsync per append (3.2 submits/s measured) |
| Snapshots | Sent whole in one message | Chunked transfer |
