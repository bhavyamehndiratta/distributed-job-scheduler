package io.github.bhavyamehndiratta.djs.wire;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.raft.AppendEntries;
import io.github.bhavyamehndiratta.djs.raft.AppendEntriesResponse;
import io.github.bhavyamehndiratta.djs.raft.InstallSnapshot;
import io.github.bhavyamehndiratta.djs.raft.InstallSnapshotResponse;
import io.github.bhavyamehndiratta.djs.raft.LogEntry;
import io.github.bhavyamehndiratta.djs.raft.RequestVote;
import io.github.bhavyamehndiratta.djs.raft.RequestVoteResponse;
import io.github.bhavyamehndiratta.djs.scheduler.ClientMessages;
import io.github.bhavyamehndiratta.djs.scheduler.JobSpec;
import io.github.bhavyamehndiratta.djs.scheduler.JobStatus;
import io.github.bhavyamehndiratta.djs.scheduler.JobView;
import io.github.bhavyamehndiratta.djs.scheduler.RunState;
import io.github.bhavyamehndiratta.djs.scheduler.RunView;
import io.github.bhavyamehndiratta.djs.scheduler.WorkerMessages;
import java.util.ArrayList;
import java.util.List;

/**
 * Text encoding of every message that crosses the network. One line per
 * message: a tag, then fields separated by '|'. A string field is '=' plus the
 * escaped text, and a null string is '^', so empty and null stay distinct.
 * Decoding is strict: unknown tags, missing or extra fields and bad numbers all
 * throw IllegalArgumentException, and the transport drops such messages.
 */
public final class MessageCodec {

    private MessageCodec() {
    }

    public static String encode(Message message) {
        W w = new W();
        switch (message) {
            case RequestVote m -> w.tag("RV").num(m.term()).node(m.candidate()).num(m.lastLogIndex())
                    .num(m.lastLogTerm()).bool(m.preVote());
            case RequestVoteResponse m -> w.tag("RVR").num(m.term()).bool(m.granted()).bool(m.preVote());
            case AppendEntries m -> {
                w.tag("AE").num(m.term()).node(m.leader()).num(m.prevLogIndex()).num(m.prevLogTerm())
                        .num(m.leaderCommit()).num(m.entries().size());
                for (LogEntry e : m.entries()) {
                    w.num(e.term()).str(e.command());
                }
                w.num(m.readSeq());
            }
            case AppendEntriesResponse m -> w.tag("AER").num(m.term()).bool(m.success()).num(m.matchIndex())
                    .num(m.conflictTerm()).num(m.conflictIndex()).num(m.readSeq());
            case InstallSnapshot m -> w.tag("IS").num(m.term()).node(m.leader()).num(m.lastIncludedIndex())
                    .num(m.lastIncludedTerm()).str(m.data());
            case InstallSnapshotResponse m -> w.tag("ISR").num(m.term()).num(m.matchIndex());
            case WorkerMessages.Poll m -> w.tag("PO").str(m.workerId());
            case io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.Members m -> w.tag("MB").num(m.requestId()).str(m.action()).str(m.nodeId())
                    .str(m.address());
            case io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.MembersReply m -> w.tag("MR").num(m.requestId()).str(m.status().name())
                    .str(m.detail()).str(m.voters()).str(m.learners());
            case WorkerMessages.Assignment m -> w.tag("AS").str(m.workerId()).str(m.runId()).num(m.token())
                    .str(m.handler()).str(m.args()).num(m.leaseTicks());
            case WorkerMessages.Heartbeat m -> w.tag("HB").str(m.workerId()).str(m.runId()).num(m.token());
            case WorkerMessages.HeartbeatAck m -> w.tag("HA").str(m.runId()).num(m.token()).bool(m.valid());
            case WorkerMessages.Started m -> w.tag("ST").str(m.workerId()).str(m.runId()).num(m.token());
            case WorkerMessages.Completed m -> w.tag("CO").str(m.workerId()).str(m.runId()).num(m.token())
                    .bool(m.success()).str(m.detail());
            case WorkerMessages.CompletedAck m -> w.tag("CA").str(m.runId()).num(m.token()).bool(m.accepted());
            case WorkerMessages.NotLeader m -> w.tag("NL").node(m.leaderHint());
            case ClientMessages.Submit m -> w.tag("SU").num(m.requestId()).str(m.jobId()).spec(m.spec())
                    .str(m.idempotencyKey());
            case ClientMessages.SubmitReply m -> w.tag("SR").num(m.requestId()).name(m.status()).str(m.jobId())
                    .str(m.detail());
            case ClientMessages.Cancel m -> w.tag("CN").num(m.requestId()).str(m.jobId());
            case ClientMessages.CancelReply m -> w.tag("CR").num(m.requestId()).name(m.status()).str(m.detail());
            case ClientMessages.GetJob m -> w.tag("GJ").num(m.requestId()).str(m.jobId());
            case ClientMessages.JobReply m -> {
                w.tag("JR").num(m.requestId()).bool(m.found()).bool(m.job() != null);
                if (m.job() != null) {
                    w.job(m.job());
                }
                w.num(m.runs().size());
                for (RunView r : m.runs()) {
                    w.run(r);
                }
            }
            case ClientMessages.ListJobs m -> w.tag("LJ").num(m.requestId());
            case ClientMessages.JobsReply m -> {
                w.tag("JL").num(m.requestId()).num(m.jobs().size());
                for (JobView j : m.jobs()) {
                    w.job(j);
                }
            }
            default -> throw new IllegalArgumentException("cannot encode " + message.getClass().getName());
        }
        return w.toString();
    }

    public static Message decode(String text) {
        try {
            return decodeUnchecked(text);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("malformed message: " + e, e);
        }
    }

    private static Message decodeUnchecked(String text) {
        R r = new R(text);
        String tag = r.tag();
        Message out = switch (tag) {
            case "RV" -> new RequestVote(r.num(), r.node(), r.num(), r.num(), r.bool());
            case "RVR" -> new RequestVoteResponse(r.num(), r.bool(), r.bool());
            case "AE" -> {
                long term = r.num();
                NodeId leader = r.node();
                long prevIndex = r.num();
                long prevTerm = r.num();
                long commit = r.num();
                int n = r.count();
                List<LogEntry> entries = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    entries.add(new LogEntry(r.num(), r.str()));
                }
                long readSeq = r.num();
                yield new AppendEntries(term, leader, prevIndex, prevTerm, entries, commit, readSeq);
            }
            case "AER" -> new AppendEntriesResponse(r.num(), r.bool(), r.num(), r.num(), r.num(), r.num());
            case "IS" -> new InstallSnapshot(r.num(), r.node(), r.num(), r.num(), r.str());
            case "ISR" -> new InstallSnapshotResponse(r.num(), r.num());
            case "PO" -> new WorkerMessages.Poll(r.str());
            case "MB" -> new io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.Members(r.num(), r.str(), r.str(), r.str());
            case "MR" -> new io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.MembersReply(r.num(), io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.Status.valueOf(r.str()), r.str(),
                    r.str(), r.str());
            case "AS" -> new WorkerMessages.Assignment(r.str(), r.str(), r.num(), r.str(), r.str(), r.num());
            case "HB" -> new WorkerMessages.Heartbeat(r.str(), r.str(), r.num());
            case "HA" -> new WorkerMessages.HeartbeatAck(r.str(), r.num(), r.bool());
            case "ST" -> new WorkerMessages.Started(r.str(), r.str(), r.num());
            case "CO" -> new WorkerMessages.Completed(r.str(), r.str(), r.num(), r.bool(), r.str());
            case "CA" -> new WorkerMessages.CompletedAck(r.str(), r.num(), r.bool());
            case "NL" -> new WorkerMessages.NotLeader(r.node());
            case "SU" -> new ClientMessages.Submit(r.num(), r.str(), r.spec(), r.str());
            case "SR" -> new ClientMessages.SubmitReply(r.num(), r.name(ClientMessages.Status.class), r.str(),
                    r.str());
            case "CN" -> new ClientMessages.Cancel(r.num(), r.str());
            case "CR" -> new ClientMessages.CancelReply(r.num(), r.name(ClientMessages.Status.class), r.str());
            case "GJ" -> new ClientMessages.GetJob(r.num(), r.str());
            case "JR" -> {
                long id = r.num();
                boolean found = r.bool();
                JobView job = r.bool() ? r.job() : null;
                int n = r.count();
                List<RunView> runs = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    runs.add(r.run());
                }
                yield new ClientMessages.JobReply(id, found, job, List.copyOf(runs));
            }
            case "LJ" -> new ClientMessages.ListJobs(r.num());
            case "JL" -> {
                long id = r.num();
                int n = r.count();
                List<JobView> jobs = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    jobs.add(r.job());
                }
                yield new ClientMessages.JobsReply(id, List.copyOf(jobs));
            }
            default -> throw new IllegalArgumentException("unknown message tag: " + tag);
        };
        r.done();
        return out;
    }

    private static final class W {
        private final StringBuilder sb = new StringBuilder();

        W tag(String t) {
            sb.append(t);
            return this;
        }

        W num(long n) {
            sb.append('|').append(n);
            return this;
        }

        W bool(boolean b) {
            sb.append('|').append(b ? '1' : '0');
            return this;
        }

        W str(String s) {
            sb.append('|');
            if (s == null) {
                sb.append('^');
                return this;
            }
            sb.append('=');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '\\' -> sb.append("\\\\");
                    case '|' -> sb.append("\\p");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    default -> sb.append(c);
                }
            }
            return this;
        }

        W node(NodeId n) {
            return str(n == null ? null : n.value());
        }

        W name(Enum<?> e) {
            return str(e.name());
        }

        W spec(JobSpec s) {
            return str(s.handler()).str(s.args()).str(s.cron()).num(s.runAt()).num(s.maxAttempts());
        }

        W job(JobView j) {
            return str(j.jobId()).spec(j.spec()).name(j.status()).num(j.createdAt()).num(j.lastFiredTime())
                    .num(j.skippedCount()).str(j.idempotencyKey());
        }

        W run(RunView r) {
            return str(r.runId()).str(r.jobId()).num(r.scheduledTime()).name(r.state()).num(r.attempts())
                    .num(r.token()).str(r.workerId()).num(r.leaseTicks()).str(r.detail());
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }

    private static final class R {
        private final String[] parts;
        private int pos = 0;

        R(String text) {
            if (text == null || text.isEmpty()) {
                throw new IllegalArgumentException("empty message");
            }
            parts = text.split("\\|", -1);
        }

        String tag() {
            pos = 1;
            return parts[0];
        }

        private String next() {
            if (pos >= parts.length) {
                throw new IllegalArgumentException("truncated message");
            }
            return parts[pos++];
        }

        long num() {
            return Long.parseLong(next());
        }

        int count() {
            long n = num();
            if (n < 0 || n > 1_000_000) {
                throw new IllegalArgumentException("bad count " + n);
            }
            return (int) n;
        }

        boolean bool() {
            String t = next();
            if (t.equals("1")) {
                return true;
            }
            if (t.equals("0")) {
                return false;
            }
            throw new IllegalArgumentException("bad boolean: " + t);
        }

        String str() {
            String t = next();
            if (t.equals("^")) {
                return null;
            }
            if (!t.startsWith("=")) {
                throw new IllegalArgumentException("bad string field");
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < t.length(); i++) {
                char c = t.charAt(i);
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (++i >= t.length()) {
                    throw new IllegalArgumentException("dangling escape");
                }
                switch (t.charAt(i)) {
                    case '\\' -> sb.append('\\');
                    case 'p' -> sb.append('|');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    default -> throw new IllegalArgumentException("bad escape");
                }
            }
            return sb.toString();
        }

        NodeId node() {
            String s = str();
            return s == null ? null : new NodeId(s);
        }

        <E extends Enum<E>> E name(Class<E> type) {
            String s = str();
            if (s == null) {
                throw new IllegalArgumentException("missing enum value");
            }
            return Enum.valueOf(type, s);
        }

        JobSpec spec() {
            String handler = str();
            String args = str();
            String cron = str();
            long runAt = num();
            int maxAttempts = Math.toIntExact(num());
            return new JobSpec(handler, args, cron, runAt, maxAttempts);
        }

        JobView job() {
            return new JobView(str(), spec(), name(JobStatus.class), num(), num(), num(), str());
        }

        RunView run() {
            return new RunView(str(), str(), num(), name(RunState.class), Math.toIntExact(num()), num(), str(),
                    num(), str());
        }

        void done() {
            if (pos != parts.length) {
                throw new IllegalArgumentException("extra fields in message");
            }
        }
    }
}
