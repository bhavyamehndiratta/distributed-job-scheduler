package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.NodeId;
import java.nio.file.Path;
import java.util.Map;

/** Runs one cluster member. Usage: node --id n1 --cluster n1=host:port,... --data DIR [--host H] [--tick-ms 100] [--lease-ticks 30] */
public final class ServerMain {

    private ServerMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Cli.options(args, 0);
        if (!o.containsKey("id") || !o.containsKey("data") || (!o.containsKey("cluster") && System.getenv("DJS_CLUSTER") == null)) {
            System.err.println("usage: node --id n1 --cluster n1=host:port,n2=host:port,... --data DIR "
                    + "[--host 127.0.0.1] [--tick-ms 100] [--lease-ticks 30] [--join true]");
            System.exit(2);
        }
        var cluster = ClusterSpec.parse(o.getOrDefault("cluster", System.getenv("DJS_CLUSTER")));
        NodeId id = new NodeId(o.get("id"));
        SchedulerServer server = new SchedulerServer(id, cluster, Path.of(o.get("data")),
                o.getOrDefault("host", "127.0.0.1"), Long.parseLong(o.getOrDefault("tick-ms", "100")),
                Long.parseLong(o.getOrDefault("lease-ticks", "30")),
                Boolean.parseBoolean(o.getOrDefault("join", "false")));
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        System.out.println("node " + id.value() + " listening on " + server.address());
        Thread.currentThread().join();
    }
}
