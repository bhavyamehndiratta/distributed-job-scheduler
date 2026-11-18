package io.github.bhavyamehndiratta.djs.raft;

/** The role a Raft node currently plays. PRE_CANDIDATE is the PreVote phase. */
public enum Role {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
