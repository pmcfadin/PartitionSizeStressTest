package com.datastax;

import com.codahale.metrics.Snapshot;

import java.util.HashMap;
import java.util.TreeMap;

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
public class Test {
    String testName;
    long rowSize;
    TreeMap<String,Snapshot> testResults;

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

    public TreeMap<String, Snapshot> getTestResults() {
        return testResults;
    }

    public Test(String testName, long rowSize) {
        this.rowSize = rowSize;
        this.testName = testName;
        testResults = new TreeMap<String, Snapshot>();
    }

}
