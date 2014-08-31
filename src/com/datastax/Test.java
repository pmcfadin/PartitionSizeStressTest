package com.datastax;

import com.codahale.metrics.Snapshot;

import java.util.HashMap;

/**
 * Created by patrick on 8/29/14.
 */
public class Test {
    String testName;
    long rowSize;
    HashMap<String,Snapshot> testResults;

    public String getTestName() {
        return testName;
    }

    public void setTestName(String testName) {
        this.testName = testName;
    }

    public long getRowSize() {
        return rowSize;
    }

    public void setRowSize(long rowSize) {
        this.rowSize = rowSize;
    }

    public HashMap<String, Snapshot> getTestResults() {
        return testResults;
    }

    public Test(String testName, long rowSize) {
        this.rowSize = rowSize;
        this.testName = testName;
        testResults = new HashMap<String, Snapshot>();
    }

}
