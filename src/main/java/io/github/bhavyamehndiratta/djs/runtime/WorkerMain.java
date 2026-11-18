package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.scheduler.EffectStore;
import java.util.Map;

/** Runs one worker. Usage: worker --id w1 --cluster n1=host:port,... [--host H] [--port 0] [--tick-ms 100] */
public final class WorkerMain {

    private WorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Cli.options(args, 0);
        if (!o.containsKey("id") || (!o.containsKey("cluster") && System.getenv("DJS_CLUSTER") == null)) {
            System.err.println("usage: worker --id w1 --cluster n1=host:port,n2=host:port,... "
                    + "[--host 127.0.0.1] [--port 0] [--tick-ms 100]");
            System.exit(2);
        }
        var cluster = ClusterSpec.parse(o.getOrDefault("cluster", System.getenv("DJS_CLUSTER")));
        NodeId id = new NodeId(o.get("id"));
        WorkerProcess worker = new WorkerProcess(id, cluster, o.getOrDefault("host", "127.0.0.1"),
                Integer.parseInt(o.getOrDefault("port", "0")), Long.parseLong(o.getOrDefault("tick-ms", "100")),
                new EffectStore());
        Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
        System.out.println("worker " + id.value() + " running");
        int last = 0;
        while (true) {
            Thread.sleep(2000);
            int total = worker.totalEffects();
            if (total != last) {
                System.out.println("worker " + id.value() + ": " + total + " effects applied");
                last = total;
            }
        }
    }
}
