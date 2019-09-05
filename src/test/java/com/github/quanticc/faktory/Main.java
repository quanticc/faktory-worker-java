package com.github.quanticc.faktory;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.downgoon.snowflake.Snowflake;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    @Test
    public void testProducerConsumer() throws URISyntaxException, IOException, FaktoryConnectionError, InterruptedException, NoSuchAlgorithmException {
        FaktoryClient client = FaktoryClient.builder()
                .withConcurrency(8)
                .withWorkerId("wrk1")
                .build();

        Snowflake s = new Snowflake(1, 1);
        Random r = new Random();

        for (int i = 0; i < 100; i++) {
            client.submit(FaktoryJob.builder()
                    .withJobType("TestJob")
                    .withJobId(String.valueOf(s.nextId()))
                    .withArgs(Arrays.asList(r.nextInt(100), r.nextInt(100)))
                    .build());
            Thread.sleep(100L);
        }

        client.register("TestJob", job -> log.info("Accepted job: {}", job));
        client.run();

        log.info("Done");
    }

    @Test
    public void testConsumer() throws URISyntaxException, IOException, FaktoryConnectionError, NoSuchAlgorithmException {
        FaktoryClient client = FaktoryClient.builder()
                .withConcurrency(8)
                .withWorkerId("wrk1")
                .build();

        client.register("TestJob", job -> log.info("Accepted job: {}", job));
        client.run();

        log.info("Done");
    }
}
