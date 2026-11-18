package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Who is in the cluster: voters (count towards elections and commits) and
 * learners (receive the log but never vote), plus the network address of nodes
 * that were added at run time. A configuration is stored in the log as a
 * command with a reserved prefix, so it replicates and commits like any other
 * entry. Each entry carries the full configuration, never a delta, and the
 * addresses ride along so every node, including one that restarts or catches up
 * from a snapshot, learns how to reach the others.
 */
public record ClusterConfig(Set<NodeId> voters, Set<NodeId> learners, Map<NodeId, String> addresses) {

    /** Marks a log command as a configuration entry. No client command may start with it. */
    static final String PREFIX = "\u0001cfg|";

    public static final ClusterConfig EMPTY = new ClusterConfig(Set.of(), Set.of());

    public ClusterConfig {
        voters = copy(voters);
        learners = copy(learners);
        for (NodeId v : voters) {
            if (learners.contains(v)) {
                throw new IllegalArgumentException(v.value() + " cannot be both voter and learner");
            }
        }
        Map<NodeId, String> kept = new TreeMap<>();
        for (Map.Entry<NodeId, String> e : addresses.entrySet()) {
            if (voters.contains(e.getKey()) || learners.contains(e.getKey())) {
                kept.put(e.getKey(), checkAddress(e.getValue()));
            }
        }
        addresses = Collections.unmodifiableMap(kept);
    }

    public ClusterConfig(Set<NodeId> voters, Set<NodeId> learners) {
        this(voters, learners, Map.of());
    }

    public static ClusterConfig of(Collection<NodeId> voters) {
        return new ClusterConfig(new TreeSet<>(voters), Set.of());
    }

    public boolean isVoter(NodeId node) {
        return voters.contains(node);
    }

    public boolean isLearner(NodeId node) {
        return learners.contains(node);
    }

    public Set<NodeId> all() {
        Set<NodeId> out = new TreeSet<>(voters);
        out.addAll(learners);
        return out;
    }

    public ClusterConfig withLearner(NodeId node, String address) {
        Set<NodeId> l = new TreeSet<>(learners);
        l.add(node);
        Map<NodeId, String> a = new TreeMap<>(addresses);
        if (address != null && !address.isBlank()) {
            a.put(node, address);
        }
        return new ClusterConfig(voters, l, a);
    }

    public ClusterConfig promoted(NodeId node) {
        Set<NodeId> v = new TreeSet<>(voters);
        v.add(node);
        Set<NodeId> l = new TreeSet<>(learners);
        l.remove(node);
        return new ClusterConfig(v, l, addresses);
    }

    public ClusterConfig without(NodeId node) {
        Set<NodeId> v = new TreeSet<>(voters);
        v.remove(node);
        Set<NodeId> l = new TreeSet<>(learners);
        l.remove(node);
        return new ClusterConfig(v, l, addresses);
    }

    public String encode() {
        StringBuilder a = new StringBuilder();
        for (Map.Entry<NodeId, String> e : addresses.entrySet()) {
            if (a.length() > 0) {
                a.append(',');
            }
            a.append(e.getKey().value()).append('=').append(e.getValue());
        }
        return PREFIX + join(voters) + "|" + join(learners) + "|" + a;
    }

    public static boolean isConfigCommand(String command) {
        return command != null && command.startsWith(PREFIX);
    }

    public static ClusterConfig decode(String command) {
        if (!isConfigCommand(command)) {
            throw new IllegalArgumentException("not a configuration entry");
        }
        String[] parts = command.substring(PREFIX.length()).split("\\|", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("malformed configuration entry");
        }
        Map<NodeId, String> addresses = new TreeMap<>();
        if (!parts[2].isEmpty()) {
            for (String item : parts[2].split(",", -1)) {
                String[] kv = item.split("=", 2);
                if (kv.length != 2) {
                    throw new IllegalArgumentException("malformed address in configuration entry");
                }
                addresses.put(new NodeId(kv[0]), kv[1]);
            }
        }
        return new ClusterConfig(parse(parts[0]), parse(parts[1]), addresses);
    }

    private static Set<NodeId> parse(String list) {
        Set<NodeId> out = new TreeSet<>();
        if (!list.isEmpty()) {
            for (String part : list.split(",", -1)) {
                out.add(new NodeId(part));
            }
        }
        return out;
    }

    private static String join(Set<NodeId> nodes) {
        StringBuilder sb = new StringBuilder();
        for (NodeId n : nodes) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(n.value());
        }
        return sb.toString();
    }

    private static Set<NodeId> copy(Set<NodeId> source) {
        Set<NodeId> out = new TreeSet<>();
        for (NodeId n : source) {
            String v = n.value();
            if (v.isEmpty()) {
                throw new IllegalArgumentException("empty node id");
            }
            for (int i = 0; i < v.length(); i++) {
                char c = v.charAt(i);
                if (c < 0x20 || c == ',' || c == '|' || c == '=') {
                    throw new IllegalArgumentException("node id not allowed in a configuration: " + v);
                }
            }
            out.add(n);
        }
        return Collections.unmodifiableSet(out);
    }

    private static String checkAddress(String address) {
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("blank address");
        }
        for (int i = 0; i < address.length(); i++) {
            char c = address.charAt(i);
            if (c < 0x20 || c == ',' || c == '|' || c == '=') {
                throw new IllegalArgumentException("address not allowed in a configuration: " + address);
            }
        }
        return address;
    }
}
