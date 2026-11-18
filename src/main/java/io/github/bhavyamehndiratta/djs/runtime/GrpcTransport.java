package io.github.bhavyamehndiratta.djs.runtime;

import io.github.bhavyamehndiratta.djs.net.Message;
import io.github.bhavyamehndiratta.djs.net.NodeId;
import io.github.bhavyamehndiratta.djs.net.Transport;
import io.github.bhavyamehndiratta.djs.wire.MessageCodec;
import io.github.bhavyamehndiratta.djs.wire.pb.Ack;
import io.github.bhavyamehndiratta.djs.wire.pb.Envelope;
import io.github.bhavyamehndiratta.djs.wire.pb.NodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * The real network transport: one gRPC server per process plus a channel per
 * peer. Sends are fire and forget, like the simulated network: a message to a
 * dead peer is dropped, and the protocol above already tolerates loss,
 * duplication and reordering. Each message carries the sender's listening
 * address, so replies can reach workers and clients that are not in the
 * static cluster list.
 */
public final class GrpcTransport implements Transport, AutoCloseable {

    private static final long SEND_DEADLINE_MILLIS = 2000;
    /** Snapshots travel in one message, so the receiving side must accept large ones. */
    private static final int MAX_MESSAGE_BYTES = 256 * 1024 * 1024;

    private final NodeId self;
    private final Map<NodeId, String> addresses = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final BiConsumer<NodeId, Message> sink;
    private final Server server;
    private final String advertiseAddress;

    /** {@code port} 0 picks a free port. */
    public GrpcTransport(NodeId self, String host, int port, Map<NodeId, String> peers,
                         BiConsumer<NodeId, Message> sink) throws IOException {
        this.self = self;
        this.sink = sink;
        this.addresses.putAll(peers);
        this.server = ServerBuilder.forPort(port).maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .addService(new Service()).build().start();
        this.advertiseAddress = host + ":" + server.getPort();
    }

    /** Learns where a node that was added at run time can be reached. */
    public void addPeer(NodeId node, String address) {
        addresses.put(node, address);
    }

    public String address() {
        return advertiseAddress;
    }

    @Override
    public void send(NodeId from, NodeId to, Message message) {
        String address = addresses.get(to);
        if (address == null) {
            return;
        }
        ManagedChannel channel = channels.computeIfAbsent(address,
                a -> ManagedChannelBuilder.forTarget(a).usePlaintext().build());
        Envelope envelope = Envelope.newBuilder()
                .setFrom(from.value())
                .setFromAddress(advertiseAddress)
                .setMessage(MessageCodec.encode(message))
                .build();
        NodeServiceGrpc.newStub(channel).withDeadlineAfter(SEND_DEADLINE_MILLIS, TimeUnit.MILLISECONDS)
                .deliver(envelope, new StreamObserver<Ack>() {
                    @Override
                    public void onNext(Ack value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        // dropped: the peer is down or slow, which the protocol treats like any lost message
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    private final class Service extends NodeServiceGrpc.NodeServiceImplBase {
        @Override
        public void deliver(Envelope request, StreamObserver<Ack> response) {
            try {
                NodeId from = new NodeId(request.getFrom());
                if (!request.getFromAddress().isEmpty()) {
                    addresses.put(from, request.getFromAddress());
                }
                sink.accept(from, MessageCodec.decode(request.getMessage()));
            } catch (IllegalArgumentException e) {
                System.err.println(self.value() + ": dropping malformed message: " + e.getMessage());
            }
            response.onNext(Ack.getDefaultInstance());
            response.onCompleted();
        }
    }

    @Override
    public void close() {
        server.shutdownNow();
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
