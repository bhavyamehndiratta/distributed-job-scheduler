package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class EffectStoreTest {

    @Test
    void aSecondAttemptForTheSameRunIsAlreadyApplied() {
        EffectStore s = new EffectStore();
        assertEquals(EffectStore.Result.APPLIED, s.apply("r", 5));
        assertEquals(EffectStore.Result.ALREADY_APPLIED, s.apply("r", 5));
        assertEquals(EffectStore.Result.ALREADY_APPLIED, s.apply("r", 9));
        assertEquals(1, s.effectCount("r"));
    }

    @Test
    void aLowerTokenAfterAHigherOneIsStale() {
        EffectStore s = new EffectStore();
        assertEquals(EffectStore.Result.APPLIED, s.apply("r", 9));
        assertEquals(EffectStore.Result.STALE, s.apply("r", 5));
        assertEquals(1, s.effectCount("r"));
    }

    @Test
    void aStaleWorkerWritingBeforeTheReplacementStillYieldsOneEffect() {
        EffectStore s = new EffectStore();
        assertEquals(EffectStore.Result.APPLIED, s.apply("r", 5));        // paused worker wakes up first
        assertEquals(EffectStore.Result.ALREADY_APPLIED, s.apply("r", 9)); // replacement arrives second
        assertEquals(1, s.effectCount("r"));
        assertTrue(s.duplicateEffects().isEmpty());
    }

    @Test
    void runsAreIndependent() {
        EffectStore s = new EffectStore();
        s.apply("a", 5);
        s.apply("b", 1);
        assertEquals(2, s.totalEffects());
        assertEquals(0, s.effectCount("c"));
    }

    @Test
    void aStoreWithoutProtectionShowsTheDuplicateTheCheckerLooksFor() {
        EffectStore naive = new EffectStore(false);
        naive.apply("r", 5);
        naive.apply("r", 9);
        assertEquals(2, naive.effectCount("r"));
        assertFalse(naive.duplicateEffects().isEmpty());
    }
}
