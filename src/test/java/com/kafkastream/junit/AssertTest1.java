package com.kafkastream.junit;
import junit.framework.AssertionFailedError;
import junit.framework.TestResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.junit.Assert.*;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@RunWith(SpringJUnit4ClassRunner.class)
public class AssertTest1 extends TestResult
{
    private int fValue1;
    private int fValue2;
    @Before
    public void setUp()
    {

    }

    @Before
    public void beforeTest()
    {
        fValue1 = 2;
        fValue2 = 3;
    }

    @Test
    @SuppressWarnings(value = "true")
    public void Test1()
    {
        List<String> list = Arrays.asList("1", "2", "3");
        assertNotNull(list);

        assertEquals(fValue1,2);
    }

    @Test(expected = ArithmeticException.class)
    public void Test2()
    {
        int a=10;
        int b=1/0;
    }

    public synchronized void addError(Test test,Throwable throwable)
    {
        super.addError((junit.framework.Test) test,throwable);
    }


    // add the failure
    public synchronized void addFailure(Test test, AssertionFailedError t)
    {
        super.addFailure((junit.framework.Test) test, t);
    }
    // Marks that the test run should stop.
    public synchronized void stop()
    {
        //stop the test here
    }
}
