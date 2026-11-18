package io.github.bhavyamehndiratta.djs.scheduler;

import java.util.StringJoiner;

/**
 * Text encoding of {@link Command} for the Raft log: fields joined by '|', with
 * '%', '|', '~', CR and LF percent-escaped and a bare '~' meaning null.
 * {@link #decode} throws IllegalArgumentException on anything malformed; the
 * state machine turns that into a deterministic rejection.
 */
public final class CommandCodec {

    private CommandCodec() {
    }

    public static String encode(Command command) {
        return switch (command) {
            case Command.SubmitJob c -> join("S", c.jobId(), c.spec().handler(), c.spec().args(),
                    c.spec().cron(), Long.toString(c.spec().runAt()), Integer.toString(c.spec().maxAttempts()),
                    c.idempotencyKey(), Long.toString(c.now()));
            case Command.CancelJob c -> join("C", c.jobId());
            case Command.FireRun c -> join("F", c.jobId(), Long.toString(c.scheduledTime()),
                    Long.toString(c.skippedCount()));
            case Command.AssignRun c -> join("A", c.runId(), c.workerId(), Long.toString(c.leaseTicks()));
            case Command.RunStarted c -> join("T", c.runId(), Long.toString(c.token()));
            case Command.RunCompleted c -> join("D", c.runId(), Long.toString(c.token()),
                    c.success() ? "1" : "0", c.detail());
            case Command.ExpireAssignment c -> join("X", c.runId(), Long.toString(c.token()));
        };
    }

    public static Command decode(String text) {
        if (text == null) {
            throw new IllegalArgumentException("null command");
        }
        String[] raw = text.split("\\|", -1);
        String[] p = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            p[i] = unescape(raw[i]);
        }
        return switch (p[0] == null ? "" : p[0]) {
            case "S" -> {
                expect(p, 9);
                yield new Command.SubmitJob(p[1],
                        new JobSpec(p[2], p[3], p[4], Long.parseLong(p[5]), Integer.parseInt(p[6])),
                        p[7], Long.parseLong(p[8]));
            }
            case "C" -> {
                expect(p, 2);
                yield new Command.CancelJob(p[1]);
            }
            case "F" -> {
                expect(p, 4);
                yield new Command.FireRun(p[1], Long.parseLong(p[2]), Long.parseLong(p[3]));
            }
            case "A" -> {
                expect(p, 4);
                yield new Command.AssignRun(p[1], p[2], Long.parseLong(p[3]));
            }
            case "T" -> {
                expect(p, 3);
                yield new Command.RunStarted(p[1], Long.parseLong(p[2]));
            }
            case "D" -> {
                expect(p, 5);
                if (!"0".equals(p[3]) && !"1".equals(p[3])) {
                    throw new IllegalArgumentException("bad success flag: " + p[3]);
                }
                yield new Command.RunCompleted(p[1], Long.parseLong(p[2]), "1".equals(p[3]), p[4]);
            }
            case "X" -> {
                expect(p, 3);
                yield new Command.ExpireAssignment(p[1], Long.parseLong(p[2]));
            }
            default -> throw new IllegalArgumentException("unknown command type: " + p[0]);
        };
    }

    private static void expect(String[] parts, int count) {
        if (parts.length != count) {
            throw new IllegalArgumentException("expected " + count + " fields, got " + parts.length);
        }
    }

    private static String join(String... parts) {
        StringJoiner joiner = new StringJoiner("|");
        for (String part : parts) {
            joiner.add(escape(part));
        }
        return joiner.toString();
    }

    private static String escape(String s) {
        if (s == null) {
            return "~";
        }
        StringBuilder sb = new StringBuilder();
        for (char ch : s.toCharArray()) {
            switch (ch) {
                case '%' -> sb.append("%25");
                case '|' -> sb.append("%7C");
                case '~' -> sb.append("%7E");
                case '\n' -> sb.append("%0A");
                case '\r' -> sb.append("%0D");
                default -> sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static String unescape(String s) {
        if (s.equals("~")) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '%') {
                if (i + 3 > s.length()) {
                    throw new IllegalArgumentException("truncated escape in: " + s);
                }
                sb.append((char) Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 2;
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }
}
