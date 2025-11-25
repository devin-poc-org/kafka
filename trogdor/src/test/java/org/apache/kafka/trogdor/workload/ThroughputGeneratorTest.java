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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120)
public class ThroughputGeneratorTest {

    @Test
    public void testConstantThroughputGeneratorProperties() {
        ConstantThroughputGenerator generator = new ConstantThroughputGenerator(50, 100);
        assertEquals(50, generator.messagesPerWindow());
        assertEquals(100, generator.windowSizeMs());
    }

    @Test
    public void testConstantThroughputGeneratorDefaultWindowSize() {
        ConstantThroughputGenerator generator = new ConstantThroughputGenerator(50, 0);
        assertEquals(50, generator.messagesPerWindow());
        assertEquals(100, generator.windowSizeMs());
    }

    @Test
    public void testConstantThroughputGeneratorNegativeWindowSize() {
        ConstantThroughputGenerator generator = new ConstantThroughputGenerator(50, -10);
        assertEquals(50, generator.messagesPerWindow());
        assertEquals(100, generator.windowSizeMs());
    }

    @Test
    public void testConstantThroughputGeneratorNoThrottleWhenZeroMessages() throws InterruptedException {
        ConstantThroughputGenerator generator = new ConstantThroughputGenerator(0, 100);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            generator.throttle();
        }
        long elapsed = System.currentTimeMillis() - startTime;
        assertTrue(elapsed < 50, "Should not throttle when messagesPerWindow is 0");
    }

    @Test
    public void testConstantThroughputGeneratorNoThrottleWhenNegativeMessages() throws InterruptedException {
        ConstantThroughputGenerator generator = new ConstantThroughputGenerator(-10, 100);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            generator.throttle();
        }
        long elapsed = System.currentTimeMillis() - startTime;
        assertTrue(elapsed < 50, "Should not throttle when messagesPerWindow is negative");
    }

    @Test
    public void testConstantThroughputGeneratorThrottles() throws InterruptedException {
        ConstantThroughputGenerator generator = new ConstantThroughputGenerator(5, 100);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            generator.throttle();
        }
        long elapsed = System.currentTimeMillis() - startTime;
        assertTrue(elapsed >= 90, "Should throttle when exceeding messagesPerWindow");
    }

    @Test
    public void testGaussianThroughputGeneratorProperties() {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(50, 5.0, 10, 100);
        assertEquals(50, generator.messagesPerWindowAverage());
        assertEquals(5.0, generator.messagesPerWindowDeviation(), 0.001);
        assertEquals(10, generator.windowsUntilRateChange());
        assertEquals(100, generator.windowSizeMs());
    }

    @Test
    public void testGaussianThroughputGeneratorDefaultWindowSize() {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(50, 5.0, 10, 0);
        assertEquals(50, generator.messagesPerWindowAverage());
        assertEquals(100, generator.windowSizeMs());
    }

    @Test
    public void testGaussianThroughputGeneratorNegativeWindowSize() {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(50, 5.0, 10, -10);
        assertEquals(50, generator.messagesPerWindowAverage());
        assertEquals(100, generator.windowSizeMs());
    }

    @Test
    public void testGaussianThroughputGeneratorThrottles() throws InterruptedException {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(5, 0.0, 1, 100);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            generator.throttle();
        }
        long elapsed = System.currentTimeMillis() - startTime;
        assertTrue(elapsed >= 90, "Should throttle when exceeding messagesPerWindowAverage");
    }

    @Test
    public void testGaussianThroughputGeneratorWithDeviation() throws InterruptedException {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(100, 10.0, 5, 50);
        for (int i = 0; i < 50; i++) {
            generator.throttle();
        }
    }

    @Test
    public void testGaussianThroughputGeneratorWindowRateChange() throws InterruptedException {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(1000, 100.0, 1, 10);
        for (int i = 0; i < 100; i++) {
            generator.throttle();
        }
    }
}
