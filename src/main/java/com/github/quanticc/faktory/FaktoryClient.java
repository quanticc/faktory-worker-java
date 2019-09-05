package com.github.quanticc.faktory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FaktoryClient {

    private static final Logger log = LoggerFactory.getLogger(FaktoryClient.class);

    private final FaktoryConnection connection;
    private final ObjectMapper objectMapper;
    private final String workerId;
    private final int concurrency;
    private final long heartbeatInterval;
    private final List<String> queues;
    private final String password;

    private final List<FaktoryTask> tasks = new ArrayList<>();
    private final List<FutureFaktoryTask> pending = new ArrayList<>();

    private ExecutorService executor;
    private Instant lastHeartbeat;
    private boolean disconnecting;
    private boolean quiet;
    private Instant disconnectAfter;

    public FaktoryClient(Builder builder) throws URISyntaxException {
        URI uri;

        if (builder.uri == null) {
            uri = new URI(faktoryUri());
        } else {
            uri = builder.uri;
        }

        this.connection = new FaktoryConnection(uri);
        this.objectMapper = Objects.requireNonNull(builder.objectMapper);
        this.workerId = Objects.requireNonNull(builder.workerId);
        this.concurrency = Math.max(1, builder.concurrency);
        this.heartbeatInterval = Math.max(1, builder.heartbeatInterval);

        List<String> queues = builder.queues;

        if (queues != null && queues.isEmpty()) {
            queues.add("default");
        }

        this.queues = queues;
        this.password = builder.password;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!disconnecting) {
                log.info("Shutdown: waiting up to 15 seconds for workers to finish current tasks");
                try {
                    disconnect(false, 15);
                } catch (IOException | FaktoryConnectionError e) {
                    log.warn("Unable to disconnect", e);
                }
            }
        }));
    }

    public static Builder builder() {
        return new Builder();
    }

    private static String faktoryUri() {
        String url = System.getenv("FAKTORY_URL");
        return url != null ? url : "tcp://localhost:7419";
    }

    private FaktoryConnectOptions buildConnectOptions() {
        FaktoryConnectOptions options = new FaktoryConnectOptions();
        try {
            options.setHostname(InetAddress.getLocalHost().toString());
        } catch (UnknownHostException e) {
        }
        options.setWid(workerId);
        return options;
    }

    public void submit(FaktoryJob job) throws IOException, FaktoryConnectionError, NoSuchAlgorithmException {
        if (!connection.isConnected()) {
            connect();
        }
        connection.send("PUSH " + objectMapper.writeValueAsString(job));
    }

    private void connect() throws IOException, FaktoryConnectionError, NoSuchAlgorithmException {
        FaktoryHandshake handshake = objectMapper.readValue(connection.handshake(), FaktoryHandshake.class);
        if (handshake.getVersion() != 2) {
            throw new FaktoryConnectionError("Unsupported version: " + handshake.getVersion());
        }
        FaktoryConnectOptions hello = buildConnectOptions();
        if (handshake.getNonce() != null && password != null) {
            hello.setPasswordHash(sha256(password + handshake.getNonce(), handshake.getIterations()));
        }
        connection.connect(objectMapper.writeValueAsString(hello));
    }

    private String sha256(String plaintext, int iterations) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] encoded = digest.digest(plaintext.getBytes(StandardCharsets.UTF_8));
        for (int i = 1; i < iterations; i++) {
            encoded = digest.digest(encoded);
        }
        return bytesToHex(encoded);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public void register(String jobType, Consumer<FaktoryJob> action) {
        FaktoryTask task = new FaktoryTask(jobType, action);
        tasks.add(task);
        log.debug("Registered task: {}", task);
    }

    public void deregister(String jobType) {
        tasks.removeAll(tasks.stream()
                .filter(task -> task.getJobType().equals(jobType))
                .peek(task -> log.debug("Unregistered task: {}", task))
                .collect(Collectors.toList()));
    }

    public void run() throws IOException, FaktoryConnectionError, NoSuchAlgorithmException {
        if (!connection.isConnected()) {
            connect();
        }

        log.debug("Creating a worker pool with concurrency of {}", this.concurrency);

        this.lastHeartbeat = Instant.now().plusSeconds(this.heartbeatInterval);

        log.info("Queues: {}", this.queues);

        while (true) {
            try {
                tick();
                if (!connection.isConnected()) {
                    break;
                }
            } catch (InterruptedException e) {
                if (disconnecting) {
                    break;
                }
                log.error("Fatal: waiting up to 15 seconds for workers to finish current tasks", e);
                disconnect(false, 15);
            }
        }

        if (connection.isConnected()) {
            log.warn("Forcing worker processes to shutdown...");
            disconnect(true, 30);
        }

        getExecutor().shutdown();
    }

    public void disconnect(boolean force, long waitForSeconds) throws IOException, FaktoryConnectionError {
        log.debug("Disconnecting force={}, wait={}", force, waitForSeconds);

        this.quiet = true;
        this.disconnecting = true;
        this.disconnectAfter = Instant.now().plusSeconds(waitForSeconds);

        if (force) {
            try {
                failAllJobs();
            } finally {
                connection.close();
            }
        }

    }

    private void tick() throws IOException, FaktoryConnectionError, InterruptedException {
        if (!pending.isEmpty()) {
            sendStatus();
        }

        if (shouldSendHeartbeat()) {
            heartbeat();
        }

        if (shouldFetchJob()) {
            String reply = connection.send("FETCH " + String.join(" ", queues));
            if (reply != null) {
                FaktoryJob job = objectMapper.readValue(reply, FaktoryJob.class);
                process(job);
            }
        } else {
            if (disconnecting) {
                if (pending.isEmpty()) { // can disconnect?
                    connection.close();
                    return;
                }

                if (Instant.now().isAfter(disconnectAfter)) {
                    disconnect(true, 30);
                }
            }
            Thread.sleep(250);
        }
    }

    private void sendStatus() throws IOException, FaktoryConnectionError {
        List<FutureFaktoryTask> toRemove = new ArrayList<>();
        for (FutureFaktoryTask task : pending) {
            if (task.getFuture().isDone()) {
                toRemove.add(task);
                try {
                    task.getFuture().get(1, TimeUnit.SECONDS);
                    ack(task.getFaktoryJob());
                } catch (InterruptedException | ExecutionException | TimeoutException | IOException | FaktoryConnectionError e) {
                    fail(task.getFaktoryJob(), e);
                    log.warn("Task failed: {}", task.getFaktoryJob().getJobId(), e);
                }
            }
        }
        pending.removeAll(toRemove);
    }

    private void process(FaktoryJob job) {
        for (FaktoryTask task : getRegisteredTasks(job)) {
            log.debug("Running task: {} from job {}", task.getJobType(), job);
            FutureFaktoryTask future = new FutureFaktoryTask(job, task,
                    new FutureTask<>(() -> {
                        try {
                            task.getAction().accept(job);
                        } catch (Throwable e) {
                            log.error("Task failed: {} with job: {}", task, job, e);
                            fail(job, e);
                        }
                        return null;
                    }));
            getExecutor().submit(future.getFuture());
            pending.add(future);
        }
    }

    private void ack(FaktoryJob job) throws IOException, FaktoryConnectionError {
        Map<String, String> payload = new HashMap<>();
        payload.put("jid", job.getJobId());
        connection.send("ACK " + objectMapper.writeValueAsString(payload));
    }

    private void fail(FaktoryJob job, Throwable error) throws IOException, FaktoryConnectionError {
        Map<String, Object> payload = new HashMap<>();
        payload.put("jid", job.getJobId());

        if (error != null) {
            payload.put("errtype", error.getClass().getSimpleName());
            payload.put("message", error.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            error.printStackTrace(pw);
            payload.put("backtrace", sw.toString().split("\n"));
        }

        connection.send("FAIL " + objectMapper.writeValueAsString(payload));
    }

    private void failAllJobs() throws IOException, FaktoryConnectionError {
        for (FutureFaktoryTask task : pending) {
            if (task.getFuture().isDone()) {
                ack(task.getFaktoryJob());
            } else {
                task.getFuture().cancel(true);
                fail(task.getFaktoryJob(), null);
            }
        }
    }

    private boolean shouldFetchJob() {
        return !disconnecting && !quiet && pending.size() < concurrency;
    }

    private boolean shouldSendHeartbeat() {
        return Instant.now().isAfter(lastHeartbeat.plusSeconds(heartbeatInterval));
    }

    private void heartbeat() throws IOException, FaktoryConnectionError {
        log.debug("Sending heartbeat for worker {}", workerId);
        Map<String, Object> payload = new HashMap<>();
        payload.put("wid", workerId);
        String reply = connection.send("BEAT " + objectMapper.writeValueAsString(payload));
        if (reply.equals("+OK")) {
            log.debug("Heartbeat acknowledged by Faktory");
        } else {
            Map<String, String> map = objectMapper.readValue(reply,
                    new TypeReference<Map<String, String>>() {
                    });
            String state = map.get("state");
            if ("quiet".equals(state)) {
                if (!quiet) {
                    log.warn("Faktory has quieted this worker, will not run any more tasks");
                    quiet = true;
                }
            } else if ("terminate".equals(state)) {
                if (!disconnecting) {
                    log.warn("Faktory has asked this worker to shutdown, will cancel any pending tasks still running 25s time");
                }
                disconnect(false, 25);
            }
        }
        lastHeartbeat = Instant.now();
    }

    private ExecutorService getExecutor() {
        if (executor == null || executor.isShutdown()) {
            log.debug("Creating a new executor");
            executor = Executors.newWorkStealingPool();
        }
        return executor;
    }

    private List<FaktoryTask> getRegisteredTasks(FaktoryJob job) {
        return tasks.stream().filter(task -> task.getJobType().equals(job.getJobType())).collect(Collectors.toList());
    }

    public static class Builder {

        private URI uri;
        private ObjectMapper objectMapper = new ObjectMapper();
        private String workerId;
        private int concurrency = 20;
        private long heartbeatInterval = 25;
        private List<String> queues = new ArrayList<>();
        private String password;

        public Builder withUri(URI uri) {
            this.uri = uri;
            return this;
        }

        public Builder withObjectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public Builder withWorkerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder withConcurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder withHeartbeatInterval(long heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder withQueues(List<String> queues) {
            this.queues = queues;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public FaktoryClient build() throws URISyntaxException {
            return new FaktoryClient(this);
        }
    }


}
