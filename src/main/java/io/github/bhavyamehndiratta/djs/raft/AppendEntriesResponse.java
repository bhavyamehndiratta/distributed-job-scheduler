package io.github.bhavyamehndiratta.djs.raft;

import io.github.bhavyamehndiratta.djs.net.Message;

/**
 * On success, {@code matchIndex} is the last index the follower now shares with
 * the leader. On failure, {@code conflictTerm} and {@code conflictIndex} let the
 * leader skip back a whole term at once: if the follower's log was too short,
 * conflictTerm is 0 and conflictIndex is its length plus one; otherwise
 * conflictTerm is the term of the mismatching entry and conflictIndex is the
 * first index the follower holds with that term. {@code readSeq} echoes the
 * request's ReadIndex sequence number. A response with the leader's term, success
 * or not, shows the follower still recognised that leader when it answered.
 */
public record AppendEntriesResponse(long term, boolean success, long matchIndex,
                                    long conflictTerm, long conflictIndex, long readSeq) implements Message {

    public AppendEntriesResponse(long term, boolean success, long matchIndex,
                                 long conflictTerm, long conflictIndex) {
        this(term, success, matchIndex, conflictTerm, conflictIndex, 0);
    }

    public AppendEntriesResponse(long term, boolean success) {
        this(term, success, 0, 0, 0, 0);
    }
}
