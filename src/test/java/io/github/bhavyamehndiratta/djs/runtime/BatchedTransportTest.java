package io.github.bhavyamehndiratta.djs.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.raft.AppendEntries;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchedTransportTest {

    @Test
    void nothingLeavesUntilFlushAndOrderIsKept() {
        List<Message> sent = new ArrayList<>();
        BatchedTransport t = new BatchedTransport((from, to, m) -> sent.add(m));
        NodeId a = new NodeId("n1");
        NodeId b = new NodeId("n2");
        Message first = new AppendEntries(1, a);
        Message second = new AppendEntries(2, a);
        t.send(a, b, first);
        t.send(a, b, second);
        assertTrue(sent.isEmpty(), "a message left before the flush");
        t.flush();
        assertEquals(List.of(first, second), sent);
        t.flush();
        assertEquals(2, sent.size(), "a flush must not resend");
    }
}
