package io.github.bhavyamehndiratta.djs.scheduler;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Five-field cron expression: minute hour day-of-month month day-of-week, in UTC.
 * Supports *, lists (1,5), ranges (1-5), and steps (*&#47;15, 10-30/5). Day of week
 * is 0-7 with both 0 and 7 meaning Sunday. As in classic cron, when both
 * day-of-month and day-of-week are restricted, a day matching either one fires.
 * Times are epoch milliseconds and always minute aligned.
 */
public final class CronExpression {

    private static final long MINUTE_MILLIS = 60_000L;
    private static final int SEARCH_YEARS = 8;

    private final String text;
    private final boolean[] minutes = new boolean[60];
    private final boolean[] hours = new boolean[24];
    private final boolean[] daysOfMonth = new boolean[32];
    private final boolean[] months = new boolean[13];
    private final boolean[] daysOfWeek = new boolean[8];
    private final boolean domStar;
    private final boolean dowStar;

    private CronExpression(String text) {
        this.text = text;
        String[] fields = text.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("cron needs 5 fields, got " + fields.length + ": " + text);
        }
        parseField(fields[0], 0, 59, minutes);
        parseField(fields[1], 0, 23, hours);
        this.domStar = parseField(fields[2], 1, 31, daysOfMonth);
        parseField(fields[3], 1, 12, months);
        this.dowStar = parseField(fields[4], 0, 7, daysOfWeek);
        if (daysOfWeek[7]) {
            daysOfWeek[0] = true;
        }
    }

    public static CronExpression parse(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("cron expression must not be blank");
        }
        return new CronExpression(text);
    }

    /** Fills {@code out} for one field and returns true if the field starts with a star. */
    private static boolean parseField(String field, int min, int max, boolean[] out) {
        for (String part : field.split(",", -1)) {
            String[] stepSplit = part.split("/", -1);
            if (stepSplit.length > 2) {
                throw new IllegalArgumentException("bad cron field: " + field);
            }
            int step = 1;
            if (stepSplit.length == 2) {
                step = number(stepSplit[1], field);
                if (step < 1) {
                    throw new IllegalArgumentException("step must be at least 1: " + field);
                }
            }
            int from;
            int to;
            String range = stepSplit[0];
            if (range.equals("*")) {
                from = min;
                to = max;
            } else if (range.contains("-")) {
                String[] ends = range.split("-", -1);
                if (ends.length != 2) {
                    throw new IllegalArgumentException("bad cron range: " + field);
                }
                from = number(ends[0], field);
                to = number(ends[1], field);
            } else {
                from = number(range, field);
                to = stepSplit.length == 2 ? max : from;
            }
            if (from < min || to > max || from > to) {
                throw new IllegalArgumentException("cron value out of range " + min + "-" + max + ": " + field);
            }
            for (int v = from; v <= to; v += step) {
                out[v] = true;
            }
        }
        return field.startsWith("*");
    }

    private static int number(String s, String field) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad number in cron field: " + field);
        }
    }

    private boolean dayMatches(LocalDate date) {
        boolean domMatch = daysOfMonth[date.getDayOfMonth()];
        boolean dowMatch = daysOfWeek[date.getDayOfWeek().getValue() % 7];
        return (domStar || dowStar) ? (domMatch && dowMatch) : (domMatch || dowMatch);
    }

    public boolean matches(long epochMillis) {
        if (epochMillis < 0 || epochMillis % MINUTE_MILLIS != 0) {
            return false;
        }
        LocalDateTime t = LocalDateTime.ofEpochSecond(epochMillis / 1000, 0, ZoneOffset.UTC);
        return months[t.getMonthValue()] && dayMatches(t.toLocalDate())
                && hours[t.getHour()] && minutes[t.getMinute()];
    }

    /** First matching minute strictly after {@code epochMillis}, or -1 if none within 8 years. */
    public long nextAfter(long epochMillis) {
        long start = (Math.floorDiv(epochMillis, MINUTE_MILLIS) + 1) * MINUTE_MILLIS;
        LocalDateTime cur = LocalDateTime.ofEpochSecond(start / 1000, 0, ZoneOffset.UTC);
        int lastYear = cur.getYear() + SEARCH_YEARS;
        while (cur.getYear() <= lastYear) {
            if (!months[cur.getMonthValue()]) {
                cur = cur.toLocalDate().withDayOfMonth(1).plusMonths(1).atStartOfDay();
            } else if (!dayMatches(cur.toLocalDate())) {
                cur = cur.toLocalDate().plusDays(1).atStartOfDay();
            } else if (!hours[cur.getHour()]) {
                cur = cur.withMinute(0).plusHours(1);
            } else if (!minutes[cur.getMinute()]) {
                cur = cur.plusMinutes(1);
            } else {
                return cur.toEpochSecond(ZoneOffset.UTC) * 1000;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return text;
    }
}
