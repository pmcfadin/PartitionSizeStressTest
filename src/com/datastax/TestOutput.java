package com.datastax;

import java.util.ArrayList;
import java.util.Set;

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
public class TestOutput {
    public TestOutput(ArrayList<Test> tests) {
        this.tests = tests;
    }

    ArrayList<Test> tests;

    public void setTests(ArrayList<Test> tests) {
        this.tests = tests;
    }

    void print95thPercentileByTestAndNumber(){

        System.out.println("Test\t\t\t\t\t95th");

        for(Test test: tests){
            System.out.print(test.getTestName() + "\t");
            for(String subTest: test.getTestResults().keySet()){
               System.out.format("%.3f\t", test.getTestResults().get(subTest).get95thPercentile()  / 1000000);
            }
            System.out.println("");
        }

    }

    void print99thPercentileByTestAndNumber(){

        System.out.println("Test\t\t\t\t\t99th");

        for(Test test: tests){
            System.out.print(test.getTestName() + "\t");
            for(String subTest: test.getTestResults().keySet()){
                System.out.format("%.3f\t", test.getTestResults().get(subTest).get99thPercentile()  / 1000000);
            }
            System.out.println("");
        }

    }

    void printStdDevPercentileByTestAndNumber(){

        System.out.println("Test\t\t\t\t\tStdDev");

        for(Test test: tests){
            System.out.print(test.getTestName() + "\t");
            for(String subTest: test.getTestResults().keySet()){
                System.out.format("%.3f\t", test.getTestResults().get(subTest).getStdDev()  / 1000000);
            }
            System.out.println("");
        }

    }
}
