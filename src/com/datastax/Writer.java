package com.datastax;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;


import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**

Copyright 2014 Patrick McFadin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

    Storage column size is 11 bytes fro the name, 15 bytes for overhead, ad 10 bytes of data = 31 Bytes.
    2114 cells per row in each index.

 */
public class Writer {

    static private final MetricRegistry metrics = new MetricRegistry();
    static private final Timer responses = metrics.timer(MetricRegistry.name(Writer.class, "responses"));

    // Page size based on 31 byte column and 64k per page
    static private final long pageSize = 2114;

    public static void main(String[] args) {
        Cluster cluster;
        Session session;
        ResultSet results;

        final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File("./"));
        reporter.start(1, TimeUnit.SECONDS);

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

        TreeMap<String, Long> rows = new TreeMap<String, Long>();

        rows.put("small-row", 100L);
        rows.put("no-col-index", pageSize + 1);
        rows.put("five-thousand", 5000L);
        rows.put("ten-thousand", 10000L);
        rows.put("hundred-thousand", 100000L);
        rows.put("one-million", 1000000L);
        rows.put("ten-million", 10000000L);
        //rows.put("hundred-million", 100000000L);
        //rows.put("one-billion", 1000000000L);

        long partition_count = 1000;

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

        System.out.print(responses.getSnapshot().get95thPercentile());

        System.out.println("Writes complete");
        System.out.print(partition_count + " rows written.");
        session.close();
        System.exit(1);
    }
}
