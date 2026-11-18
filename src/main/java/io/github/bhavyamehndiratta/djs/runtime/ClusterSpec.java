package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.SortedMap;
import java.util.TreeMap;

/** Parses "n1=host:port,n2=host:port,..." into a sorted map. Every process must use the same list. */
public final class ClusterSpec {

    private ClusterSpec() {
    }

    public static SortedMap<NodeId, String> parse(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("cluster list is empty");
        }
        SortedMap<NodeId, String> out = new TreeMap<>();
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2 || kv[0].isBlank() || !kv[1].contains(":")) {
                throw new IllegalArgumentException("bad cluster entry (want id=host:port): " + part);
            }
            port(kv[1].trim());
            if (out.put(new NodeId(kv[0].trim()), kv[1].trim()) != null) {
                throw new IllegalArgumentException("duplicate node id: " + kv[0].trim());
            }
        }
        return out;
    }

    public static int port(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("address needs host:port: " + address);
        }
        try {
            int port = Integer.parseInt(address.substring(colon + 1));
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port out of range: " + address);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad port in " + address);
        }
    }
}
