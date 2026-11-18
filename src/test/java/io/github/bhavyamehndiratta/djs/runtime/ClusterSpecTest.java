package io.github.bhavyamehndiratta.djs.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import org.junit.jupiter.api.Test;

class ClusterSpecTest {

    @Test
    void parsesAnOrderedList() {
        var m = ClusterSpec.parse("n2=localhost:7002, n1=127.0.0.1:7001");
        assertEquals(2, m.size());
        assertEquals(new NodeId("n1"), m.firstKey());
        assertEquals("localhost:7002", m.get(new NodeId("n2")));
        assertEquals(7001, ClusterSpec.port(m.get(new NodeId("n1"))));
    }

    @Test
    void rejectsBadLists() {
        for (String bad : new String[] {"", "n1", "n1=host", "n1=host:x", "n1=host:70000", "=h:1",
                "n1=h:1,n1=h:2"}) {
            assertThrows(IllegalArgumentException.class, () -> ClusterSpec.parse(bad), bad);
        }
    }
}
