package com.kafkastream;
import org.assertj.core.api.AbstractCharSequenceAssert;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
public class AssertTest
{
    public static void main(String[] args)
    {
        List<String> list = Arrays.asList("1", "2", "3");

        assertThat(list).contains("4");
    }
}
