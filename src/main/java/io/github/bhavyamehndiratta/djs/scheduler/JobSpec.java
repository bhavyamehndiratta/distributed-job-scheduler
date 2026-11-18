package io.github.bhavyamehndiratta.djs.scheduler;

/**
 * What to run and when. A job names a registered handler (never a shell
 * command) and its arguments. It is either one-shot, firing once at
 * {@code runAt} (epoch milliseconds), or recurring on a cron expression.
 * {@code maxAttempts} is the total number of tries a single run gets.
 */
public record JobSpec(String handler, String args, String cron, long runAt, int maxAttempts) {

    public JobSpec {
        if (handler == null || handler.isBlank()) {
            throw new IllegalArgumentException("handler must not be blank");
        }
        if (args == null) {
            args = "";
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (cron != null) {
            CronExpression.parse(cron);
            if (runAt != 0) {
                throw new IllegalArgumentException("a cron job has no runAt");
            }
        } else if (runAt < 0) {
            throw new IllegalArgumentException("runAt must not be negative");
        }
    }

    public static JobSpec once(String handler, String args, long runAt, int maxAttempts) {
        return new JobSpec(handler, args, null, runAt, maxAttempts);
    }

    public static JobSpec recurring(String handler, String args, String cron, int maxAttempts) {
        if (cron == null) {
            throw new IllegalArgumentException("cron must not be null");
        }
        return new JobSpec(handler, args, cron, 0, maxAttempts);
    }

    public boolean isRecurring() {
        return cron != null;
    }
}
