package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class CronExpressionTest {

    private static long utc(int year, int month, int day, int hour, int minute) {
        return LocalDateTime.of(year, month, day, hour, minute).toEpochSecond(ZoneOffset.UTC) * 1000;
    }

    @Test
    void everyMinuteFiresOnTheNextMinute() {
        CronExpression c = CronExpression.parse("* * * * *");
        assertEquals(60_000, c.nextAfter(0));
        assertEquals(60_000, c.nextAfter(59_999));
        assertEquals(120_000, c.nextAfter(60_000));
    }

    @Test
    void stepsAndRangesAndLists() {
        CronExpression c = CronExpression.parse("*/15 9-10 * * *");
        assertEquals(utc(2026, 3, 4, 9, 0), c.nextAfter(utc(2026, 3, 4, 8, 59)));
        assertEquals(utc(2026, 3, 4, 9, 15), c.nextAfter(utc(2026, 3, 4, 9, 0)));
        assertEquals(utc(2026, 3, 4, 10, 45), c.nextAfter(utc(2026, 3, 4, 10, 30)));
        assertEquals(utc(2026, 3, 5, 9, 0), c.nextAfter(utc(2026, 3, 4, 10, 45)));
        CronExpression list = CronExpression.parse("5,50 0 * * *");
        assertEquals(utc(2026, 3, 4, 0, 50), list.nextAfter(utc(2026, 3, 4, 0, 5)));
        CronExpression ranged = CronExpression.parse("10-30/10 * * * *");
        assertTrue(ranged.matches(utc(2026, 1, 1, 5, 10)));
        assertTrue(ranged.matches(utc(2026, 1, 1, 5, 20)));
        assertTrue(ranged.matches(utc(2026, 1, 1, 5, 30)));
        assertFalse(ranged.matches(utc(2026, 1, 1, 5, 15)));
    }

    @Test
    void mondayAtNineIsFoundAcrossDays() {
        CronExpression c = CronExpression.parse("0 9 * * 1");
        long monday = utc(2026, 11, 16, 9, 0); // 2026-11-16 is a Monday
        assertEquals(monday, c.nextAfter(utc(2026, 11, 10, 0, 0)));
        assertEquals(monday + 7L * 24 * 3600 * 1000, c.nextAfter(monday));
    }

    @Test
    void sevenAndZeroAreBothSunday() {
        long sunday = utc(2026, 11, 22, 0, 0); // a Sunday
        assertTrue(CronExpression.parse("0 0 * * 0").matches(sunday));
        assertTrue(CronExpression.parse("0 0 * * 7").matches(sunday));
        assertFalse(CronExpression.parse("0 0 * * 7").matches(sunday + 86_400_000L));
    }

    @Test
    void leapDayIsFound() {
        CronExpression c = CronExpression.parse("30 2 29 2 *");
        assertEquals(utc(2028, 2, 29, 2, 30), c.nextAfter(utc(2026, 11, 17, 0, 0)));
    }

    @Test
    void whenBothDayFieldsAreRestrictedEitherOneMatches() {
        CronExpression c = CronExpression.parse("0 0 1 * 1"); // the 1st of the month OR any Monday
        assertTrue(c.matches(utc(2026, 12, 1, 0, 0)));  // a Tuesday, but the 1st
        assertTrue(c.matches(utc(2026, 11, 16, 0, 0))); // a Monday, not the 1st
        assertFalse(c.matches(utc(2026, 11, 17, 0, 0))); // a Tuesday, not the 1st
    }

    @Test
    void matchesRequiresAMinuteBoundary() {
        CronExpression c = CronExpression.parse("* * * * *");
        assertTrue(c.matches(60_000));
        assertFalse(c.matches(60_001));
        assertFalse(c.matches(-60_000));
    }

    @Test
    void impossibleDatesGiveNoNextFire() {
        assertEquals(-1, CronExpression.parse("0 0 31 2 *").nextAfter(0));
    }

    @Test
    void badExpressionsAreRejected() {
        for (String bad : new String[] {"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *",
                "* * * 13 *", "* * * * 8", "*/0 * * * *", "a * * * *", "5-1 * * * *", "1/2/3 * * * *"}) {
            assertThrows(IllegalArgumentException.class, () -> CronExpression.parse(bad), bad);
        }
    }
}
