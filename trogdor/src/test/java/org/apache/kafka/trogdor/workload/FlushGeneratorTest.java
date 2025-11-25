/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Timeout(value = 120)
public class FlushGeneratorTest {

    @Test
    public void testConstantFlushGeneratorProperties() {
        ConstantFlushGenerator generator = new ConstantFlushGenerator(16);
        assertEquals(16, generator.messagesPerFlush());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConstantFlushGeneratorFlushesAtThreshold() {
        ConstantFlushGenerator generator = new ConstantFlushGenerator(5);
        KafkaProducer<byte[], byte[]> mockProducer = mock(KafkaProducer.class);

        for (int i = 0; i < 4; i++) {
            generator.increment(mockProducer);
        }
        verify(mockProducer, times(0)).flush();

        generator.increment(mockProducer);
        verify(mockProducer, times(1)).flush();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConstantFlushGeneratorMultipleFlushes() {
        ConstantFlushGenerator generator = new ConstantFlushGenerator(3);
        KafkaProducer<byte[], byte[]> mockProducer = mock(KafkaProducer.class);

        for (int i = 0; i < 9; i++) {
            generator.increment(mockProducer);
        }
        verify(mockProducer, times(3)).flush();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConstantFlushGeneratorResetsAfterFlush() {
        ConstantFlushGenerator generator = new ConstantFlushGenerator(2);
        KafkaProducer<byte[], byte[]> mockProducer = mock(KafkaProducer.class);

        generator.increment(mockProducer);
        generator.increment(mockProducer);
        verify(mockProducer, times(1)).flush();

        generator.increment(mockProducer);
        verify(mockProducer, times(1)).flush();

        generator.increment(mockProducer);
        verify(mockProducer, times(2)).flush();
    }

    @Test
    public void testGaussianFlushGeneratorProperties() {
        GaussianFlushGenerator generator = new GaussianFlushGenerator(16, 4.0);
        assertEquals(16, generator.messagesPerFlushAverage());
        assertEquals(4.0, generator.messagesPerFlushDeviation(), 0.001);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGaussianFlushGeneratorFlushes() {
        GaussianFlushGenerator generator = new GaussianFlushGenerator(5, 0.0);
        KafkaProducer<byte[], byte[]> mockProducer = mock(KafkaProducer.class);

        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            flushCount.incrementAndGet();
            return null;
        }).when(mockProducer).flush();

        for (int i = 0; i < 100; i++) {
            generator.increment(mockProducer);
        }

        assertTrue(flushCount.get() >= 10, "Should have flushed at least 10 times for 100 messages with average of 5");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGaussianFlushGeneratorWithDeviation() {
        GaussianFlushGenerator generator = new GaussianFlushGenerator(10, 2.0);
        KafkaProducer<byte[], byte[]> mockProducer = mock(KafkaProducer.class);

        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            flushCount.incrementAndGet();
            return null;
        }).when(mockProducer).flush();

        for (int i = 0; i < 200; i++) {
            generator.increment(mockProducer);
        }

        assertTrue(flushCount.get() >= 10, "Should have flushed multiple times");
        assertTrue(flushCount.get() <= 50, "Should not flush too frequently");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGaussianFlushGeneratorMinimumFlushSize() {
        GaussianFlushGenerator generator = new GaussianFlushGenerator(1, 100.0);
        KafkaProducer<byte[], byte[]> mockProducer = mock(KafkaProducer.class);

        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            flushCount.incrementAndGet();
            return null;
        }).when(mockProducer).flush();

        for (int i = 0; i < 100; i++) {
            generator.increment(mockProducer);
        }

        assertTrue(flushCount.get() >= 1, "Should have flushed at least once");
    }
}
