package com.kafkastream.junit;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class AssertDemoTestSuiteRunner
{
    public static void main(String[] args)
    {
        Result result=JUnitCore.runClasses(AssertDemoTestSuite.class);
        for(Failure failure:result.getFailures())
        {
            System.out.println("Failure Message-> "+failure.toString());
            System.out.println("Thrown Exception -> "+failure.getException());
        }

        System.out.println(result.wasSuccessful());
    }
}
