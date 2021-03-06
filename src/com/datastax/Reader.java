package com.datastax;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;

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
 */

public class Reader {

    static private Session session;

    static private BoundStatement select100thCellStatement;

    // Page size based on 31 byte column and 64k per page
    static private final long pageSize = 2114;
    static private SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");

    public static void main(String[] args) {

        Cluster cluster;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster
                .builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .build();

        session = cluster.connect("wide_test");

        PreparedStatement select100thCell = session.prepare("select random_data from wide1 where partition_name = ? AND partition_cell_number = ?");

        select100thCellStatement = new BoundStatement(select100thCell);

        ArrayList<Test> tests = new ArrayList<Test>();

        tests.add(new Test("small-row", 100));
        tests.add(new Test("no-col-index", pageSize + 1));
        tests.add(new Test("five-thousand", 5000));
        tests.add(new Test("ten-thousand", 10000));
        tests.add(new Test("hundred-thousand", 100000));
        tests.add(new Test("one-million", 1000000));
        tests.add(new Test("ten-million", 10000000));
        tests.add(new Test("hundred-million", 10000000));
        //tests.add(new Test("one-billion", 1000000000));

        System.out.println("Starting queries");

        // Cycle through each test
        for (Test test : tests) {
            System.out.printf("Partion: %-20s Test 1...", test.getTestName());
            test1(test);

            System.out.print(" Test 2...");
            test2(test);

            System.out.print(" Test 3...");
            test3(test);

            System.out.print(" Test 4...");
            test4(test);

            System.out.print(" Test 5...");
            test5(test);

            System.out.print(" Test 6...");
            test6(test);

            System.out.print(" Test 7...");
            test7(test);

            System.out.print(" Test 8...");
            test8(test);

            System.out.println(" Test 9...");
            test9(test);
        }

        System.out.println("Finished queries.");

        TestOutput out = new TestOutput(tests);

        out.print95thPercentileByTestAndNumber();

        out.print99thPercentileByTestAndNumber();

        out.printStdDevPercentileByTestAndNumber();

        session.close();
        System.exit(1);
    }

    // 100 named columns from beginning of row
    static void test1(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        ResultSet rs;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 100; i++) {

                final Timer.Context context = select100responses.time();

                rs = session.execute(select100thCellStatement.bind(test.getTestName(), i).enableTracing());

                context.stop();

                //printStackTrace(rs.getExecutionInfo());
            }
        }

        test.getTestResults().put("Test 1", select100responses.getSnapshot());

    }

    // 100 named columns from end of row
    static void test2(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        for (int j = 0; j < 10; j++) {

            for (long i = test.getRowSize(); i > test.getRowSize() - 100; i--) {

                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), i));

                context.stop();
            }
        }

        test.getTestResults().put("Test 2", select100responses.getSnapshot());

        Snapshot snap = select100responses.getSnapshot();
    }

    // 100 named columns from middle of row
    static void test3(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        long offset = 100;

        // Account for an offset equal to a cluster size
        if (test.getRowSize() < 100) {

            offset = (test.getRowSize() / 2);
        }

        for (int j = 0; j < 10; j++) {

            for (long i = (test.getRowSize() / 2); i > (test.getRowSize() / 2) - offset; i--) {

                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), i));

                context.stop();
            }
        }
        test.getTestResults().put("Test 3", select100responses.getSnapshot());

    }

    // Grab first 2 named columns from 50 random places on 2114 cell pages
    static void test4(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        // How many 2114 cell combinations do we have?
        long combinations = test.getRowSize() % pageSize;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 50; i++) {

                long page = pageSize * nextLong(combinations);

                // Get the first column
                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), page));

                context.stop();

                // Get the very next column
                final Timer.Context context2 = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), page + 1));

                context2.stop();
            }
        }
        test.getTestResults().put("Test 4", select100responses.getSnapshot());
    }

    // Grab last 2 named columns from 50 random places on 2114 cell pages
    static void test5(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        // How many 2114 cell combinations do we have?
        long combinations = test.getRowSize() % pageSize;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 50; i++) {

                long page = pageSize * nextLong(combinations);

                // Get the first column
                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), page - 1));

                context.stop();

                // Get the very next column
                final Timer.Context context2 = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), page - 2));

                context2.stop();
            }
        }
        test.getTestResults().put("Test 5", select100responses.getSnapshot());

    }

    // Grab 2 random named columns from 50 random places on 2114 cell pages
    static void test6(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        // How many 2114 cell combinations do we have?
        long combinations = test.getRowSize() % pageSize;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 50; i++) {

                long page = pageSize * nextLong(combinations);
                long column1 = page + nextLong(pageSize);
                long column2 = page + nextLong(pageSize);

                // Get the first column
                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), column1));

                context.stop();

                // Get the very next column
                final Timer.Context context2 = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), column2));

                context2.stop();
            }
        }
        test.getTestResults().put("Test 6", select100responses.getSnapshot());
    }

    // Grab first column from 100 random places on 2114 cell pages
    static void test7(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        // How many 2114 cell combinations do we have?
        long combinations = test.getRowSize() % pageSize;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 100; i++) {

                long page = pageSize * nextLong(combinations);

                // Get the first column
                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), page));

                context.stop();
            }
        }
        test.getTestResults().put("Test 7", select100responses.getSnapshot());

    }

    // Grab last column from 100 random places on 2114 cell pages
    static void test8(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        // How many 2114 cell combinations do we have?
        long combinations = test.getRowSize() % pageSize;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 100; i++) {

                long page = pageSize * nextLong(combinations);

                // Get the first column
                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), (page + (pageSize - 1))));

                context.stop();
            }
        }
        test.getTestResults().put("Test 8", select100responses.getSnapshot());

    }

    // Grab last column from 100 random places on 2114 cell pages
    static void test9(Test test) {
        final MetricRegistry metrics = new MetricRegistry();
        final Timer select100responses = metrics.timer(MetricRegistry.name(Reader.class, "select100responses"));

        // How many 2114 cell combinations do we have?
        long combinations = test.getRowSize() % pageSize;

        for (int j = 0; j < 10; j++) {

            for (long i = 0; i < 100; i++) {

                long page = pageSize * nextLong(combinations);

                long column = page + nextLong(pageSize);

                // Get the first column
                final Timer.Context context = select100responses.time();

                session.execute(select100thCellStatement.bind(test.getTestName(), column));

                context.stop();
            }
        }
        test.getTestResults().put("Test 9", select100responses.getSnapshot());
    }

    static long nextLong(long n) {
        Random rng = new Random();
        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (rng.nextLong() << 1) >>> 1;
            val = bits % n;
        } while (bits - val + (n - 1) < 0L);
        return val;
    }

    static void printStackTrace(ExecutionInfo info) {

        QueryTrace queryTrace = info.getQueryTrace();

        System.out.printf("\n\nTrace id: %s\n\n", queryTrace.getTraceId());
        System.out.println("----------------------------------------------------------------------------+--------------+------------+--------------");
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            System.out.printf("%75s | %12s | %10s | %12s\n", event.getDescription(),
                    format.format(event.getTimestamp()),
                    event.getSource(), event.getSourceElapsedMicros());

        }

    }
}