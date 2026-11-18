package io.github.bhavyamehndiratta.djs.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.Random;
import org.junit.jupiter.api.Test;

class MessageCodecTest {

    private static final String NASTY = "a|b\\c\nd\re=^\u00e9\u4e2d \\p\\n|";
    private static final NodeId N1 = new NodeId("n1");

    private static JobView job() {
        return new JobView("j|1", JobSpec.once("effect", NASTY, 1000, 3), JobStatus.ACTIVE, 5, 6, 7, "key|1");
    }

    private static RunView run() {
        return new RunView("j|1@1000", "j|1", 1000, RunState.RUNNING, 2, 44, "w1", 30, NASTY);
    }

    private static List<Message> samples() {
        return List.of(
                new RequestVote(3, N1, 10, 2, true),
                new RequestVote(3, N1, 0, 0, false),
                new RequestVoteResponse(3, true, false),
                new AppendEntries(4, N1),
                new AppendEntries(4, N1, 9, 3, List.of(LogEntry.noop(4), new LogEntry(4, NASTY),
                        new LogEntry(4, ""), new LogEntry(4, "plain")), 8),
                new AppendEntriesResponse(4, true, 12, 0, 0),
                new AppendEntriesResponse(4, false, 0, 3, 7),
                new InstallSnapshot(5, N1, 40, 4, NASTY),
                new InstallSnapshot(5, N1, 40, 4, ""),
                new InstallSnapshotResponse(5, 40),
                new io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.Members(9, "ADD", "n4", "127.0.0.1:7004"),
                new io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.Members(10, "LIST", "", ""),
                new io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.MembersReply(9, io.github.bhavyamehndiratta.djs.scheduler.ClientMessages.Status.ACCEPTED, NASTY, "n1,n2,n3", ""),
                new WorkerMessages.Poll("w1"),
                new WorkerMessages.Assignment("w1", "j@1", 99, "effect", NASTY, 30),
                new WorkerMessages.Heartbeat("w1", "j@1", 99),
                new WorkerMessages.HeartbeatAck("j@1", 99, false),
                new WorkerMessages.Started("w1", "j@1", 99),
                new WorkerMessages.Completed("w1", "j@1", 99, true, NASTY),
                new WorkerMessages.CompletedAck("j@1", 99, true),
                new WorkerMessages.NotLeader(N1),
                new WorkerMessages.NotLeader(null),
                new ClientMessages.Submit(7, "job", JobSpec.once("effect", NASTY, 5000, 3), "key"),
                new ClientMessages.Submit(7, "job", JobSpec.recurring("effect", "", "*/5 * * * *", 2), null),
                new ClientMessages.SubmitReply(7, ClientMessages.Status.DUPLICATE, "job", "DUPLICATE"),
                new ClientMessages.Cancel(8, "job"),
                new ClientMessages.CancelReply(8, ClientMessages.Status.REJECTED, "UNKNOWN"),
                new ClientMessages.GetJob(9, "job"),
                new ClientMessages.JobReply(9, true, job(), List.of(run(), run())),
                new ClientMessages.JobReply(9, false, null, List.of()),
                new ClientMessages.ListJobs(10),
                new ClientMessages.JobsReply(10, List.of(job(), job())),
                new ClientMessages.JobsReply(10, List.of()));
    }

    @Test
    void everyMessageTypeRoundTripsExactly() {
        for (Message m : samples()) {
            String wire = MessageCodec.encode(m);
            assertFalse(wire.contains("\n"), "one line per message: " + m);
            assertEquals(m, MessageCodec.decode(wire), wire);
        }
    }

    @Test
    void randomStringsRoundTrip() {
        Random rnd = new Random(5);
        String alphabet = "ab|\\\n\r=^p0 \u00e9\u4e2d";
        for (int i = 0; i < 2000; i++) {
            StringBuilder sb = new StringBuilder();
            int len = rnd.nextInt(12);
            for (int j = 0; j < len; j++) {
                sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
            }
            Message m = new WorkerMessages.Completed("w", sb.toString(), rnd.nextLong(), rnd.nextBoolean(),
                    sb.toString());
            assertEquals(m, MessageCodec.decode(MessageCodec.encode(m)));
        }
    }

    @Test
    void malformedInputThrowsIllegalArgumentExceptionAndNothingElse() {
        for (String bad : new String[] {"", "ZZ|1", "RV", "RV|1", "RV|x|=n1|1|1|0", "RV|1|=n1|1|1|2",
                "RV|1|=n1|1|1|0|extra", "PO|^x", "PO|=a\\", "PO|=a\\q", "SR|1|NOPE|=j|=d", "AE|1|=n1|0|0|0|-1",
                "SU|1|=j|=effect|=|^|0|0|=k", "HA|=r|99"}) {
            assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode(bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode(null));
    }

    @Test
    void randomGarbageNeverEscapesAsAnotherExceptionType() {
        Random rnd = new Random(11);
        String[] tags = {"RV", "RVR", "AE", "AER", "PO", "AS", "HB", "HA", "ST", "CO", "CA", "NL", "SU", "SR", "CN",
                "CR", "GJ", "JR", "LJ", "JL", "XX"};
        String alphabet = "01x^=|\\ pn-9";
        for (int i = 0; i < 5000; i++) {
            StringBuilder sb = new StringBuilder(tags[rnd.nextInt(tags.length)]);
            int fields = rnd.nextInt(12);
            for (int f = 0; f < fields; f++) {
                sb.append('|');
                int len = rnd.nextInt(4);
                for (int j = 0; j < len; j++) {
                    sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
                }
            }
            try {
                MessageCodec.decode(sb.toString());
            } catch (IllegalArgumentException expected) {
                // fine
            }
        }
        List<String> unused = new ArrayList<>();
        assertEquals(0, unused.size());
    }
}
