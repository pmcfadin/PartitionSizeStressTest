package com.datastax;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/*
    Storage column size is 11 bytes fro the name, 15 bytes for overhead, ad 10 bytes of data = 31 Bytes.
    2114 cells per row in each index.

 */
public class Writer {

    static private final MetricRegistry metrics = new MetricRegistry();
    static private final Timer responses = metrics.timer(MetricRegistry.name(Writer.class, "responses"));


    public static void main(String[] args) {
        Cluster cluster;
        Session session;
        ResultSet results;

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(10, TimeUnit.SECONDS);

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster
                .builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .build();
        session = cluster.connect("wide_test");

        PreparedStatement wide1_insert = session.prepare("INSERT INTO wide1 (partition_name, partition_cell_number, random_data) VALUES (?,?,?);");
        BoundStatement wide1boundStatement = new BoundStatement(wide1_insert);
        Random random = new Random();

        HashMap<String, Long> rows = new HashMap<String, Long>();

        rows.put("small-row", new Long(100));
        rows.put("no-col-index", new Long(1200));
        rows.put("five-thousand", new Long(5000));
        rows.put("ten-thousand", new Long(10000));
        rows.put("hundred-thousand", new Long(100000));
        rows.put("one-million", new Long(1000000));
        rows.put("ten-million", new Long(10000000));
        rows.put("hundred-million", new Long(100000000));
        //rows.put("one-billion", new Long(1000000000));

        long partition_count = 1000;
        long cell_count = 1000;

        System.out.println("Writing test data into cluster");
        // Cycle through each partition
        for (String rowName: rows.keySet() ){
            System.out.println("Inserting into row " + rowName);

            // Cycle through each partition cell
            for (long j = 0; j < rows.get(rowName); j++) {

                // Time the response
                final Timer.Context context = responses.time();

                session.executeAsync(wide1boundStatement.bind(rowName, j, random.nextInt()));

                context.stop();
            }
        }

        System.out.println("Writes complete");
        System.out.print(partition_count + " rows written.");
        session.close();
        System.exit(1);
    }
}
