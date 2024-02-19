package org.example;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;

public class HandlerImpl implements Handler {

    private static final int WORKERS_COUNT = 10;

    private final Client client;

    private final List<Worker> workers = new ArrayList<>();

    public HandlerImpl(Client client) {
        this.client = client;

        ExecutorService executor = Executors.newFixedThreadPool(WORKERS_COUNT + 1);

        DeadLettersWorker deadLettersWorker = new DeadLettersWorker(client, timeout());
        executor.execute(deadLettersWorker);

        for (int i = 0; i < WORKERS_COUNT; i++) {
            Worker worker = new Worker(client, timeout(), deadLettersWorker);
            workers.add(worker);
            executor.execute(worker);
        }
    }

    @Override
    public Duration timeout() {
        return Duration.ofSeconds(2);
    }

    @Override
    public void performOperation() {
        while (true) {
            Event event = client.readData(); // lock main thread until client has data to read
            for (int i = 0; i < event.recipients().size(); i++) {
                workers.get(i % WORKERS_COUNT)
                        .enqueue(new Message(event.recipients().get(i), event.payload()));
            }
        }
    }

    static class Worker implements Runnable {

        private final Client client;
        private final Duration sendTimeout;

        private final DeadLettersWorker deadLettersWorker;

        private final BlockingQueue<Message> outbox = new LinkedBlockingDeque<>();

        public Worker(Client client, Duration sendTimeout, DeadLettersWorker deadLettersWorker) {
            this.client = client;
            this.sendTimeout = sendTimeout;
            this.deadLettersWorker = deadLettersWorker;
        }

        public void enqueue(Message message) {
            outbox.add(message);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Message message = outbox.take(); // lock worker until it's outbox has available message to read

                    // send message
                    Result resp = client.sendData(message.address, message.payload);
                    if (resp == Result.REJECTED) {
                        // enqueue deadletter
                        message.ttl = Instant.now().plus(sendTimeout);
                        deadLettersWorker.enqueue(message);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class DeadLettersWorker implements Runnable {

        private final Client client;
        private final Duration sendTimeout;

        private final PriorityBlockingQueue<Message> deadLetters = new PriorityBlockingQueue<>(100, (first, second) -> {
            if (first.getTtl().isBefore(second.getTtl())) {
                return -1;
            } else if (first.getTtl().isAfter(second.getTtl())) {
                return 1;
            }
            return 0;
        });

        public DeadLettersWorker(Client client, Duration sendTimeout) {
            this.client = client;
            this.sendTimeout = sendTimeout;
        }

        public void enqueue(Message message) {
            deadLetters.add(message);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Message message = deadLetters.take(); // block deadletters worker until we have dead letters
                    Instant now = Instant.now();

                    if (message.getTtl().isBefore(now)) {
                        Thread.sleep(now.toEpochMilli() - message.getTtl().toEpochMilli());
                    }

                    // try to resend message
                    Result resp = client.sendData(message.address, message.payload);
                    if (resp == Result.REJECTED) {
                        // enqueue deadletter again
                        message.ttl = Instant.now().plus(sendTimeout);
                        this.enqueue(message);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class Message {
        private final Address address;
        private final Payload payload;
        private Instant ttl = null;

        Message(Address address, Payload payload) {
            this.address = address;
            this.payload = payload;
        }

        public Instant getTtl() {
            return ttl;
        }

        public void setTtl(Instant ttl) {
            this.ttl = ttl;
        }
    }
}
