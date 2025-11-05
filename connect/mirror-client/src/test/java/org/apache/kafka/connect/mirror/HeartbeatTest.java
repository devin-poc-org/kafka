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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeartbeatTest {

    @Test
    public void testHeartbeatConstruction() {
        Heartbeat heartbeat = new Heartbeat("source1", "target1", 123456789L);
        
        assertEquals("source1", heartbeat.sourceClusterAlias());
        assertEquals("target1", heartbeat.targetClusterAlias());
        assertEquals(123456789L, heartbeat.timestamp());
    }

    @Test
    public void testToString() {
        Heartbeat heartbeat = new Heartbeat("source-cluster", "target-cluster", 999L);
        
        String str = heartbeat.toString();
        assertTrue(str.contains("source-cluster"));
        assertTrue(str.contains("target-cluster"));
        assertTrue(str.contains("999"));
    }

    @Test
    public void testSerialization() {
        Heartbeat heartbeat = new Heartbeat("source1", "target1", 123456L);
        
        byte[] keyBytes = heartbeat.recordKey();
        byte[] valueBytes = heartbeat.recordValue();
        
        assertNotNull(keyBytes);
        assertNotNull(valueBytes);
        assertTrue(keyBytes.length > 0);
        assertTrue(valueBytes.length > 0);
    }

    @Test
    public void testDeserialization() {
        Heartbeat original = new Heartbeat("cluster-a", "cluster-b", 987654321L);
        
        byte[] key = original.recordKey();
        byte[] value = original.recordValue();
        
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "heartbeat-topic", 0, 0L, key, value
        );
        
        Heartbeat deserialized = Heartbeat.deserializeRecord(record);
        
        assertEquals(original.sourceClusterAlias(), deserialized.sourceClusterAlias());
        assertEquals(original.targetClusterAlias(), deserialized.targetClusterAlias());
        assertEquals(original.timestamp(), deserialized.timestamp());
    }

    @Test
    public void testSerializeKey() {
        Heartbeat heartbeat = new Heartbeat("src", "tgt", 100L);
        
        ByteBuffer buffer = heartbeat.serializeKey();
        assertNotNull(buffer);
        assertTrue(buffer.remaining() > 0);
    }

    @Test
    public void testSerializeValue() {
        Heartbeat heartbeat = new Heartbeat("src", "tgt", 200L);
        
        ByteBuffer buffer = heartbeat.serializeValue(Heartbeat.VERSION);
        assertNotNull(buffer);
        assertTrue(buffer.remaining() > 0);
    }

    @Test
    public void testConnectPartition() {
        Heartbeat heartbeat = new Heartbeat("source-x", "target-y", 300L);
        
        Map<String, ?> partition = heartbeat.connectPartition();
        assertEquals("source-x", partition.get(Heartbeat.SOURCE_CLUSTER_ALIAS_KEY));
        assertEquals("target-y", partition.get(Heartbeat.TARGET_CLUSTER_ALIAS_KEY));
    }

    @Test
    public void testSerializationRoundTrip() {
        long timestamp = System.currentTimeMillis();
        Heartbeat original = new Heartbeat("primary", "backup", timestamp);
        
        byte[] key = original.recordKey();
        byte[] value = original.recordValue();
        
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "test-topic", 0, 0L, key, value
        );
        
        Heartbeat roundtrip = Heartbeat.deserializeRecord(record);
        
        assertEquals("primary", roundtrip.sourceClusterAlias());
        assertEquals("backup", roundtrip.targetClusterAlias());
        assertEquals(timestamp, roundtrip.timestamp());
    }

    @Test
    public void testDifferentTimestamps() {
        Heartbeat h1 = new Heartbeat("source", "target", 1000L);
        Heartbeat h2 = new Heartbeat("source", "target", 2000L);
        
        assertNotEquals(h1.timestamp(), h2.timestamp());
    }

    @Test
    public void testDifferentClusters() {
        Heartbeat h1 = new Heartbeat("source1", "target1", 1000L);
        Heartbeat h2 = new Heartbeat("source2", "target2", 1000L);
        
        assertNotEquals(h1.sourceClusterAlias(), h2.sourceClusterAlias());
        assertNotEquals(h1.targetClusterAlias(), h2.targetClusterAlias());
    }

    @Test
    public void testMultipleSerializations() {
        Heartbeat heartbeat = new Heartbeat("s", "t", 999L);
        
        byte[] key1 = heartbeat.recordKey();
        byte[] key2 = heartbeat.recordKey();
        byte[] value1 = heartbeat.recordValue();
        byte[] value2 = heartbeat.recordValue();
        
        assertArrayEquals(key1, key2);
        assertArrayEquals(value1, value2);
    }
}
