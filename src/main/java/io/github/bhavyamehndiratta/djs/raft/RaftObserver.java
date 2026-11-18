package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.NodeId;

/** Hook for tests and the chaos checker to watch role changes as they happen. */
@FunctionalInterface
public interface RaftObserver {

    RaftObserver NONE = (node, role, term) -> { };

    void onRoleChange(NodeId node, Role role, long term);
}
