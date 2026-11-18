package io.github.bhavyamehndiratta.djs.scheduler;

import io.github.bhavyamehndiratta.djs.raft.LogEntry;
import io.github.bhavyamehndiratta.djs.raft.StateMachine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The replicated scheduler. A pure, deterministic function of the committed
 * log: it never reads a clock or a random source, uses ordered maps only, and
 * turns undecodable entries into a deterministic rejection instead of
 * throwing, so every node that applies the same entries reaches the same
 * state. Time only enters through values the leader stamped into commands.
 *
 * <p>Fencing: an assignment's token is the log index of its AssignRun entry,
 * which is unique and strictly increasing. Reports carrying any other token
 * are rejected, so a worker presumed dead cannot change a run that has been
 * reassigned.
 *
 * <p>Not yet bounded: finished runs and idempotency records are kept until
 * snapshots and pruning arrive in a later stage.
 */
public final class SchedulerStateMachine implements StateMachine {

    public static final long DEFAULT_IDEMPOTENCY_RETENTION_MILLIS = 24L * 60 * 60 * 1000;

    /** Most missed fire times {@link #dueFires} will count for one job. */
    private static final int MAX_MISSED_SCAN = 1_000_000;

    /** Observes every applied command with its outcome. Used by the leader to answer clients. */
    @FunctionalInterface
    public interface Listener {
        void applied(long index, Command command, Outcome outcome);
    }

    private static final class Job {
        String id;
        JobSpec spec;
        JobStatus status = JobStatus.ACTIVE;
        long createdAt;
        long lastFired = -1;
        long skipped = 0;
        String idempotencyKey;
    }

    private static final class Run {
        String id;
        String jobId;
        long scheduledTime;
        RunState state = RunState.SCHEDULED;
        int attempts = 0;
        long token = 0;
        String worker = null;
        long leaseTicks = 0;
        String detail = "";
    }

    private record KeyRecord(String jobId, long storedAt) {
    }

    private final Map<String, Job> jobs = new TreeMap<>();
    private final Map<String, Run> runs = new TreeMap<>();
    private final Map<String, KeyRecord> keys = new LinkedHashMap<>();
    private final long retentionMillis;
    private final Listener listener;

    public SchedulerStateMachine() {
        this(DEFAULT_IDEMPOTENCY_RETENTION_MILLIS, (index, command, outcome) -> { });
    }

    public SchedulerStateMachine(long idempotencyRetentionMillis, Listener listener) {
        if (idempotencyRetentionMillis < 1) {
            throw new IllegalArgumentException("retention must be positive");
        }
        this.retentionMillis = idempotencyRetentionMillis;
        this.listener = listener;
    }

    @Override
    public void apply(long index, LogEntry entry) {
        if (entry.isNoop() || io.github.bhavyamehndiratta.djs.raft.ClusterConfig.isConfigCommand(entry.command())) {
            return; // no-ops and cluster configuration entries carry nothing for the scheduler
        }
        Command command;
        try {
            command = CommandCodec.decode(entry.command());
        } catch (IllegalArgumentException e) {
            listener.applied(index, null, Outcome.MALFORMED);
            return;
        }
        Outcome outcome = switch (command) {
            case Command.SubmitJob c -> submit(c);
            case Command.CancelJob c -> cancel(c);
            case Command.FireRun c -> fire(c);
            case Command.AssignRun c -> assign(index, c);
            case Command.RunStarted c -> started(c);
            case Command.RunCompleted c -> completed(c);
            case Command.ExpireAssignment c -> expire(c);
        };
        listener.applied(index, command, outcome);
    }

    // ---- commands ----

    private Outcome submit(Command.SubmitJob c) {
        sweepKeys(c.now());
        if (jobs.containsKey(c.jobId())) {
            return Outcome.DUPLICATE;
        }
        if (c.idempotencyKey() != null && keys.containsKey(c.idempotencyKey())) {
            return Outcome.DUPLICATE;
        }
        Job job = new Job();
        job.id = c.jobId();
        job.spec = c.spec();
        job.createdAt = c.now();
        job.idempotencyKey = c.idempotencyKey();
        jobs.put(job.id, job);
        if (c.idempotencyKey() != null) {
            keys.put(c.idempotencyKey(), new KeyRecord(job.id, c.now()));
        }
        return Outcome.APPLIED;
    }

    /** Drops keys older than the retention window. Uses only leader-stamped time, so every node agrees. */
    private void sweepKeys(long now) {
        Iterator<KeyRecord> it = keys.values().iterator();
        while (it.hasNext()) {
            KeyRecord oldest = it.next();
            if (oldest.storedAt() + retentionMillis > now) {
                break;
            }
            it.remove();
        }
    }

    private Outcome cancel(Command.CancelJob c) {
        Job job = jobs.get(c.jobId());
        if (job == null) {
            return Outcome.UNKNOWN;
        }
        if (job.status == JobStatus.CANCELLED) {
            return Outcome.DUPLICATE;
        }
        job.status = JobStatus.CANCELLED;
        for (Run run : runs.values()) {
            if (run.jobId.equals(job.id) && run.state == RunState.SCHEDULED) {
                run.state = RunState.CANCELLED;
            }
        }
        return Outcome.APPLIED;
    }

    private Outcome fire(Command.FireRun c) {
        Job job = jobs.get(c.jobId());
        if (job == null) {
            return Outcome.UNKNOWN;
        }
        if (job.status != JobStatus.ACTIVE) {
            return Outcome.REJECTED;
        }
        if (job.spec.isRecurring()) {
            if (!CronExpression.parse(job.spec.cron()).matches(c.scheduledTime())) {
                return Outcome.REJECTED;
            }
        } else if (c.scheduledTime() != job.spec.runAt()) {
            return Outcome.REJECTED;
        }
        if (c.scheduledTime() <= job.lastFired) {
            return Outcome.DUPLICATE;
        }
        Run run = new Run();
        run.jobId = job.id;
        run.scheduledTime = c.scheduledTime();
        run.id = job.id + "@" + run.scheduledTime;
        runs.put(run.id, run);
        job.lastFired = c.scheduledTime();
        job.skipped += c.skippedCount();
        return Outcome.APPLIED;
    }

    private Outcome assign(long index, Command.AssignRun c) {
        Run run = runs.get(c.runId());
        if (run == null) {
            return Outcome.UNKNOWN;
        }
        if (run.state != RunState.SCHEDULED || jobs.get(run.jobId).status != JobStatus.ACTIVE) {
            return Outcome.REJECTED;
        }
        run.state = RunState.ASSIGNED;
        run.token = index;
        run.worker = c.workerId();
        run.leaseTicks = c.leaseTicks();
        run.attempts++;
        run.detail = "";
        return Outcome.APPLIED;
    }

    private Outcome started(Command.RunStarted c) {
        Run run = runs.get(c.runId());
        if (run == null) {
            return Outcome.UNKNOWN;
        }
        if (run.token != c.token()) {
            return Outcome.STALE_TOKEN;
        }
        if (run.state == RunState.RUNNING) {
            return Outcome.DUPLICATE;
        }
        if (run.state != RunState.ASSIGNED) {
            return Outcome.REJECTED;
        }
        run.state = RunState.RUNNING;
        return Outcome.APPLIED;
    }

    private Outcome completed(Command.RunCompleted c) {
        Run run = runs.get(c.runId());
        if (run == null) {
            return Outcome.UNKNOWN;
        }
        if (run.token != c.token()) {
            return Outcome.STALE_TOKEN;
        }
        if (run.state.isTerminal()) {
            return Outcome.DUPLICATE;
        }
        if (run.state != RunState.ASSIGNED && run.state != RunState.RUNNING) {
            return Outcome.REJECTED;
        }
        run.detail = c.detail();
        if (c.success()) {
            run.state = RunState.SUCCEEDED;
        } else {
            retryOrFail(run);
        }
        return Outcome.APPLIED;
    }

    private Outcome expire(Command.ExpireAssignment c) {
        Run run = runs.get(c.runId());
        if (run == null) {
            return Outcome.UNKNOWN;
        }
        if (run.token != c.token()) {
            return Outcome.STALE_TOKEN;
        }
        if (run.state != RunState.ASSIGNED && run.state != RunState.RUNNING) {
            return Outcome.REJECTED;
        }
        run.detail = "lease expired";
        retryOrFail(run);
        return Outcome.APPLIED;
    }

    /** After a failed or expired attempt: queue another try, or stop for good. */
    private void retryOrFail(Run run) {
        Job job = jobs.get(run.jobId);
        if (job.status == JobStatus.CANCELLED) {
            run.state = RunState.CANCELLED;
        } else if (run.attempts < job.spec.maxAttempts()) {
            run.state = RunState.SCHEDULED;
            run.token = 0;
            run.worker = null;
        } else {
            run.state = RunState.FAILED;
        }
    }

    // ---- snapshots ----

    @Override
    public boolean supportsSnapshots() {
        return true;
    }

    /**
     * The whole state as text: a version line, then one line per job (J), run (R)
     * and idempotency key (K), tab separated, in the maps' own deterministic order.
     * The job line reuses the SubmitJob encoding for the spec.
     */
    @Override
    public String snapshot() {
        StringBuilder sb = new StringBuilder("V1\n");
        for (Job j : jobs.values()) {
            sb.append("J\t")
                    .append(esc(CommandCodec.encode(new Command.SubmitJob(j.id, j.spec, j.idempotencyKey, j.createdAt))))
                    .append('\t').append(j.status).append('\t').append(j.lastFired)
                    .append('\t').append(j.skipped).append('\n');
        }
        for (Run r : runs.values()) {
            sb.append("R\t").append(esc(r.id)).append('\t').append(esc(r.jobId)).append('\t')
                    .append(r.scheduledTime).append('\t').append(r.state).append('\t').append(r.attempts)
                    .append('\t').append(r.token).append('\t').append(nul(r.worker)).append('\t')
                    .append(r.leaseTicks).append('\t').append(esc(r.detail)).append('\n');
        }
        for (Map.Entry<String, KeyRecord> k : keys.entrySet()) {
            sb.append("K\t").append(esc(k.getKey())).append('\t').append(esc(k.getValue().jobId()))
                    .append('\t').append(k.getValue().storedAt()).append('\n');
        }
        return sb.toString();
    }

    /** Replaces all state. Parses everything first, so a bad snapshot leaves the old state untouched. */
    @Override
    public void restore(String snapshot) {
        Map<String, Job> newJobs = new TreeMap<>();
        Map<String, Run> newRuns = new TreeMap<>();
        Map<String, KeyRecord> newKeys = new LinkedHashMap<>();
        String[] lines = snapshot.split("\n", -1);
        if (!lines[0].equals("V1")) {
            throw new IllegalArgumentException("unknown snapshot format");
        }
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isEmpty()) {
                continue;
            }
            String[] f = lines[i].split("\t", -1);
            switch (f[0]) {
                case "J" -> {
                    Command.SubmitJob c = (Command.SubmitJob) CommandCodec.decode(unesc(f[1]));
                    Job j = new Job();
                    j.id = c.jobId();
                    j.spec = c.spec();
                    j.createdAt = c.now();
                    j.idempotencyKey = c.idempotencyKey();
                    j.status = JobStatus.valueOf(f[2]);
                    j.lastFired = Long.parseLong(f[3]);
                    j.skipped = Long.parseLong(f[4]);
                    newJobs.put(j.id, j);
                }
                case "R" -> {
                    Run r = new Run();
                    r.id = unesc(f[1]);
                    r.jobId = unesc(f[2]);
                    r.scheduledTime = Long.parseLong(f[3]);
                    r.state = RunState.valueOf(f[4]);
                    r.attempts = Integer.parseInt(f[5]);
                    r.token = Long.parseLong(f[6]);
                    r.worker = denul(f[7]);
                    r.leaseTicks = Long.parseLong(f[8]);
                    r.detail = unesc(f[9]);
                    newRuns.put(r.id, r);
                }
                case "K" -> newKeys.put(unesc(f[1]), new KeyRecord(unesc(f[2]), Long.parseLong(f[3])));
                default -> throw new IllegalArgumentException("bad snapshot line: " + f[0]);
            }
        }
        jobs.clear();
        jobs.putAll(newJobs);
        runs.clear();
        runs.putAll(newRuns);
        keys.clear();
        keys.putAll(newKeys);
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r");
    }

    private static String unesc(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            char n = s.charAt(++i);
            sb.append(switch (n) {
                case 'n' -> '\n';
                case 't' -> '\t';
                case 'r' -> '\r';
                case '\\' -> '\\';
                default -> throw new IllegalArgumentException("bad escape in snapshot");
            });
        }
        return sb.toString();
    }

    private static String nul(String s) {
        return s == null ? "^" : "=" + esc(s);
    }

    private static String denul(String s) {
        if (s.equals("^")) {
            return null;
        }
        if (s.startsWith("=")) {
            return unesc(s.substring(1));
        }
        throw new IllegalArgumentException("bad nullable field in snapshot");
    }

    // ---- queries ----

    public Optional<JobView> job(String jobId) {
        return Optional.ofNullable(jobs.get(jobId)).map(SchedulerStateMachine::view);
    }

    public Optional<RunView> run(String runId) {
        return Optional.ofNullable(runs.get(runId)).map(SchedulerStateMachine::view);
    }

    /** The job created under this idempotency key, if the key is still remembered. */
    public Optional<String> jobIdForKey(String idempotencyKey) {
        return Optional.ofNullable(keys.get(idempotencyKey)).map(KeyRecord::jobId);
    }

    public List<JobView> jobs() {
        List<JobView> out = new ArrayList<>();
        jobs.values().forEach(j -> out.add(view(j)));
        return out;
    }

    public List<RunView> runs() {
        List<RunView> out = new ArrayList<>();
        runs.values().forEach(r -> out.add(view(r)));
        return out;
    }

    /** Runs in a state, oldest scheduled time first, then by id. */
    public List<RunView> runsInState(RunState state) {
        List<RunView> out = new ArrayList<>();
        for (Run r : runs.values()) {
            if (r.state == state) {
                out.add(view(r));
            }
        }
        out.sort((a, b) -> a.scheduledTime() != b.scheduledTime()
                ? Long.compare(a.scheduledTime(), b.scheduledTime())
                : a.runId().compareTo(b.runId()));
        return out;
    }

    /**
     * The FireRun commands a leader should propose at time {@code now}. Applies
     * the misfire policy: for a recurring job, only the most recent missed fire
     * time is fired and the older ones are counted as skipped; a one-shot job
     * whose time has passed fires once. Read-only, so it is safe to call
     * repeatedly; a command that was already proposed is simply a duplicate.
     */
    public List<Command.FireRun> dueFires(long now) {
        List<Command.FireRun> due = new ArrayList<>();
        for (Job job : jobs.values()) {
            if (job.status != JobStatus.ACTIVE) {
                continue;
            }
            if (!job.spec.isRecurring()) {
                if (job.lastFired < 0 && job.spec.runAt() <= now) {
                    due.add(new Command.FireRun(job.id, job.spec.runAt(), 0));
                }
                continue;
            }
            CronExpression cron = CronExpression.parse(job.spec.cron());
            long t = cron.nextAfter(Math.max(job.lastFired, job.createdAt));
            long latest = -1;
            long count = 0;
            while (t != -1 && t <= now && count < MAX_MISSED_SCAN) {
                latest = t;
                count++;
                t = cron.nextAfter(t);
            }
            if (count > 0) {
                due.add(new Command.FireRun(job.id, latest, count - 1));
            }
        }
        return due;
    }

    /** Canonical text of the whole state, for comparing replicas and replays. */
    public String digest() {
        StringBuilder sb = new StringBuilder();
        for (Job j : jobs.values()) {
            sb.append("job ").append(j.id).append(' ').append(j.spec).append(' ').append(j.status)
                    .append(" created=").append(j.createdAt).append(" lastFired=").append(j.lastFired)
                    .append(" skipped=").append(j.skipped).append(" key=").append(j.idempotencyKey).append('\n');
        }
        for (Run r : runs.values()) {
            sb.append("run ").append(r.id).append(' ').append(r.state).append(" attempts=").append(r.attempts)
                    .append(" token=").append(r.token).append(" worker=").append(r.worker)
                    .append(" lease=").append(r.leaseTicks).append(" detail=").append(r.detail).append('\n');
        }
        keys.forEach((k, v) -> sb.append("key ").append(k).append(" -> ").append(v.jobId())
                .append(" at ").append(v.storedAt()).append('\n'));
        return sb.toString();
    }

    private static JobView view(Job j) {
        return new JobView(j.id, j.spec, j.status, j.createdAt, j.lastFired, j.skipped, j.idempotencyKey);
    }

    private static RunView view(Run r) {
        return new RunView(r.id, r.jobId, r.scheduledTime, r.state, r.attempts, r.token, r.worker,
                r.leaseTicks, r.detail);
    }
}
