package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.scheduler.Client;
import io.github.bhavyamehndiratta.djs.scheduler.ClientMessages;
import io.github.bhavyamehndiratta.djs.scheduler.JobSpec;
import io.github.bhavyamehndiratta.djs.scheduler.JobView;
import io.github.bhavyamehndiratta.djs.scheduler.RunView;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Command line client.
 * <pre>
 * djs [--cluster n1=host:port,...] submit --handler H [--args A] (--in SECONDS | --at EPOCH_MILLIS | --cron "* * * * *")
 *                                          [--attempts N] [--id JOBID] [--key KEY]
 * djs [--cluster ...] status JOBID
 * djs [--cluster ...] cancel JOBID
 * djs [--cluster ...] list
 * djs [--cluster ...] member list|add|promote|remove [--node N] [--address HOST:PORT]
 * </pre>
 * The cluster list can also come from the DJS_CLUSTER environment variable. Without --key the job id is the
 * idempotency key, so re-running the same command with the same --id never creates a second job.
 */
public final class Cli {

    private Cli() {
    }

    /** Parses {@code --key value} pairs starting at {@code from}; bare words are ignored here. */
    static Map<String, String> options(String[] args, int from) {
        Map<String, String> out = new HashMap<>();
        for (int i = from; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for " + args[i]);
                }
                out.put(args[i].substring(2), args[++i]);
            }
        }
        return out;
    }

    public static void main(String[] args) {
        int code;
        try {
            code = run(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            code = 2;
        } catch (IllegalStateException e) {
            System.err.println("error: " + e.getMessage());
            code = 1;
        } catch (Exception e) {
            System.err.println("error: " + e);
            code = 1;
        }
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        int cmdAt = 0;
        while (cmdAt < args.length && args[cmdAt].startsWith("--")) {
            cmdAt += 2;
        }
        if (cmdAt >= args.length) {
            System.err.println("usage: djs [--cluster n1=host:port,...] submit|status|cancel|list ...");
            return 2;
        }
        String command = args[cmdAt];
        Map<String, String> o = options(args, 0);
        String spec = o.getOrDefault("cluster", System.getenv("DJS_CLUSTER"));
        var cluster = ClusterSpec.parse(spec);
        try (RemoteClient client = new RemoteClient(cluster, o.getOrDefault("host", "127.0.0.1"), 100, 20_000)) {
            switch (command) {
                case "submit" -> {
                    JobSpec js = buildSpec(o);
                    String jobId = o.getOrDefault("id", "job-" + UUID.randomUUID().toString().substring(0, 8));
                    Client.SubmitResult r = client.submit(jobId, js, o.getOrDefault("key", jobId));
                    System.out.println(r.status() + " " + r.jobId());
                    return r.status() == ClientMessages.Status.REJECTED ? 1 : 0;
                }
                case "status" -> {
                    Client.JobInfo info = client.getJob(positional(args, cmdAt));
                    if (!info.found()) {
                        System.out.println("no such job");
                        return 1;
                    }
                    printJob(info.job());
                    for (RunView r : info.runs()) {
                        System.out.println("  run " + r.runId() + "  " + r.state() + "  attempts=" + r.attempts()
                                + "  worker=" + (r.workerId() == null ? "-" : r.workerId()) + "  token=" + r.token());
                    }
                    return 0;
                }
                case "cancel" -> {
                    Client.CancelResult r = client.cancel(positional(args, cmdAt));
                    System.out.println(r.status() + " " + r.detail());
                    return r.status() == ClientMessages.Status.REJECTED ? 1 : 0;
                }
                case "member" -> {
                    if (cmdAt + 1 >= args.length || args[cmdAt + 1].startsWith("--")) {
                        throw new IllegalArgumentException("member needs list, add, promote or remove");
                    }
                    String action = args[cmdAt + 1].toUpperCase();
                    Client.MembersResult r = client.members(action, o.getOrDefault("node", ""),
                            o.getOrDefault("address", ""));
                    System.out.println(r.status() + " " + r.detail());
                    System.out.println("voters: " + r.voters() + "  learners: " + r.learners());
                    return r.status() == ClientMessages.Status.REJECTED ? 1 : 0;
                }
                case "list" -> {
                    List<JobView> jobs = client.listJobs();
                    for (JobView j : jobs) {
                        printJob(j);
                    }
                    System.out.println(jobs.size() + " job(s)");
                    return 0;
                }
                default -> {
                    System.err.println("unknown command: " + command);
                    return 2;
                }
            }
        }
    }

    private static String positional(String[] args, int cmdAt) {
        if (cmdAt + 1 >= args.length || args[cmdAt + 1].startsWith("--")) {
            throw new IllegalArgumentException("missing JOBID");
        }
        return args[cmdAt + 1];
    }

    private static JobSpec buildSpec(Map<String, String> o) {
        String handler = o.get("handler");
        if (handler == null) {
            throw new IllegalArgumentException("submit needs --handler");
        }
        int when = (o.containsKey("in") ? 1 : 0) + (o.containsKey("at") ? 1 : 0) + (o.containsKey("cron") ? 1 : 0);
        if (when != 1) {
            throw new IllegalArgumentException("give exactly one of --in, --at, --cron");
        }
        String args = o.getOrDefault("args", "");
        int attempts = Integer.parseInt(o.getOrDefault("attempts", "3"));
        if (o.containsKey("cron")) {
            return JobSpec.recurring(handler, args, o.get("cron"), attempts);
        }
        long runAt = o.containsKey("at") ? Long.parseLong(o.get("at"))
                : System.currentTimeMillis() + Long.parseLong(o.get("in")) * 1000;
        return JobSpec.once(handler, args, runAt, attempts);
    }

    private static void printJob(JobView j) {
        String when = j.spec().isRecurring() ? "cron " + j.spec().cron()
                : "once at " + Instant.ofEpochMilli(j.spec().runAt());
        System.out.println("job " + j.jobId() + "  " + j.status() + "  handler=" + j.spec().handler() + " args="
                + j.spec().args() + "  " + when + "  attempts<=" + j.spec().maxAttempts());
    }
}
