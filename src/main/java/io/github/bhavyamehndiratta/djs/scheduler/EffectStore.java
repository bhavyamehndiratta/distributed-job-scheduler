package io.github.bhavyamehndiratta.djs.scheduler;

import java.util.Map;
import java.util.TreeMap;

/**
 * Stand-in for the outside world that jobs act on, and the place where
 * exactly-once EFFECTS are enforced. It applies two rules per run id:
 * <ol>
 *   <li>At most one effect: a second attempt gets ALREADY_APPLIED, which a
 *       worker treats as success.</li>
 *   <li>No writes below the highest token seen: a worker holding an older
 *       token gets STALE.</li>
 * </ol>
 * The dedupe rule is what makes effects exactly-once. The token rule alone is
 * not enough: a paused worker with token 5 can write before the replacement
 * with token 9 has reached the store, so the store must also remember that the
 * effect already happened. The chaos checker reads {@link #effectCount}.
 *
 * <p>{@code enforce=false} turns both rules off. It exists only so tests can
 * show that the checks fail on a store that does not protect itself.
 */
public final class EffectStore {

    public enum Result { APPLIED, ALREADY_APPLIED, STALE }

    private static final class Entry {
        long highestToken = 0;
        int effects = 0;
    }

    private final boolean enforce;
    private final Map<String, Entry> entries = new TreeMap<>();

    public EffectStore() {
        this(true);
    }

    public EffectStore(boolean enforce) {
        this.enforce = enforce;
    }

    public Result apply(String runId, long token) {
        Entry e = entries.computeIfAbsent(runId, k -> new Entry());
        if (!enforce) {
            e.effects++;
            return Result.APPLIED;
        }
        if (token < e.highestToken) {
            return Result.STALE;
        }
        e.highestToken = token;
        if (e.effects > 0) {
            return Result.ALREADY_APPLIED;
        }
        e.effects = 1;
        return Result.APPLIED;
    }

    public int effectCount(String runId) {
        Entry e = entries.get(runId);
        return e == null ? 0 : e.effects;
    }

    public int totalEffects() {
        int total = 0;
        for (Entry e : entries.values()) {
            total += e.effects;
        }
        return total;
    }

    /** Run ids with more than one effect. Must always be empty. */
    public Map<String, Integer> duplicateEffects() {
        Map<String, Integer> out = new TreeMap<>();
        entries.forEach((id, e) -> {
            if (e.effects > 1) {
                out.put(id, e.effects);
            }
        });
        return out;
    }
}
