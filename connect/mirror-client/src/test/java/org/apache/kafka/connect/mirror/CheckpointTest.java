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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CheckpointTest {

    @Test
    public void testCheckpointConstruction() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 100L, 200L, "metadata");
        
        assertEquals("group1", checkpoint.consumerGroupId());
        assertEquals(tp, checkpoint.topicPartition());
        assertEquals(100L, checkpoint.upstreamOffset());
        assertEquals(200L, checkpoint.downstreamOffset());
        assertEquals("metadata", checkpoint.metadata());
    }

    @Test
    public void testOffsetAndMetadata() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 100L, 200L, "test-metadata");
        
        OffsetAndMetadata offsetAndMetadata = checkpoint.offsetAndMetadata();
        assertEquals(200L, offsetAndMetadata.offset());
        assertEquals("test-metadata", offsetAndMetadata.metadata());
    }

    @Test
    public void testToString() {
        TopicPartition tp = new TopicPartition("test-topic", 5);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 100L, 200L, "metadata");
        
        String str = checkpoint.toString();
        assertTrue(str.contains("group1"));
        assertTrue(str.contains("test-topic"));
        assertTrue(str.contains("100"));
        assertTrue(str.contains("200"));
    }

    @Test
    public void testSerialization() {
        TopicPartition tp = new TopicPartition("test-topic", 3);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 100L, 200L, "test-meta");
        
        byte[] keyBytes = checkpoint.recordKey();
        byte[] valueBytes = checkpoint.recordValue();
        
        assertNotNull(keyBytes);
        assertNotNull(valueBytes);
        assertTrue(keyBytes.length > 0);
        assertTrue(valueBytes.length > 0);
    }

    @Test
    public void testDeserialization() {
        TopicPartition tp = new TopicPartition("my-topic", 7);
        Checkpoint original = new Checkpoint("consumer-group", tp, 500L, 600L, "my-metadata");
        
        byte[] key = original.recordKey();
        byte[] value = original.recordValue();
        
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "checkpoint-topic", 0, 0L, key, value
        );
        
        Checkpoint deserialized = Checkpoint.deserializeRecord(record);
        
        assertEquals(original.consumerGroupId(), deserialized.consumerGroupId());
        assertEquals(original.topicPartition(), deserialized.topicPartition());
        assertEquals(original.upstreamOffset(), deserialized.upstreamOffset());
        assertEquals(original.downstreamOffset(), deserialized.downstreamOffset());
        assertEquals(original.metadata(), deserialized.metadata());
    }

    @Test
    public void testSerializeKey() {
        TopicPartition tp = new TopicPartition("topic1", 2);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 10L, 20L, "meta");
        
        ByteBuffer buffer = checkpoint.serializeKey();
        assertNotNull(buffer);
        assertTrue(buffer.remaining() > 0);
    }

    @Test
    public void testSerializeValue() {
        TopicPartition tp = new TopicPartition("topic1", 2);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 10L, 20L, "meta");
        
        ByteBuffer buffer = checkpoint.serializeValue(Checkpoint.VERSION);
        assertNotNull(buffer);
        assertTrue(buffer.remaining() > 0);
    }

    @Test
    public void testConnectPartition() {
        TopicPartition tp = new TopicPartition("topic1", 4);
        Checkpoint checkpoint = new Checkpoint("group1", tp, 10L, 20L, "meta");
        
        Map<String, ?> partition = checkpoint.connectPartition();
        assertEquals("group1", partition.get(Checkpoint.CONSUMER_GROUP_ID_KEY));
        assertEquals("topic1", partition.get(Checkpoint.TOPIC_KEY));
        assertEquals(4, partition.get(Checkpoint.PARTITION_KEY));
    }

    @Test
    public void testUnwrapGroup() {
        Map<String, Object> partition = Map.of(
            Checkpoint.CONSUMER_GROUP_ID_KEY, "test-group",
            Checkpoint.TOPIC_KEY, "test-topic",
            Checkpoint.PARTITION_KEY, 0
        );
        
        String group = Checkpoint.unwrapGroup(partition);
        assertEquals("test-group", group);
    }

    @Test
    public void testEquals() {
        TopicPartition tp = new TopicPartition("topic", 1);
        Checkpoint c1 = new Checkpoint("group", tp, 100L, 200L, "meta");
        Checkpoint c2 = new Checkpoint("group", tp, 100L, 200L, "meta");
        Checkpoint c3 = new Checkpoint("group", tp, 100L, 201L, "meta");
        
        assertEquals(c1, c2);
        assertNotEquals(c1, c3);
        assertNotEquals(c1, null);
        assertNotEquals(c1, "string");
    }

    @Test
    public void testHashCode() {
        TopicPartition tp = new TopicPartition("topic", 1);
        Checkpoint c1 = new Checkpoint("group", tp, 100L, 200L, "meta");
        Checkpoint c2 = new Checkpoint("group", tp, 100L, 200L, "meta");
        
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    public void testSerializationRoundTrip() {
        TopicPartition tp = new TopicPartition("roundtrip-topic", 3);
        Checkpoint original = new Checkpoint("my-group", tp, 1000L, 2000L, "test");
        
        byte[] key = original.recordKey();
        byte[] value = original.recordValue();
        
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "checkpoint-topic", 0, 0L, key, value
        );
        
        Checkpoint roundtrip = Checkpoint.deserializeRecord(record);
        
        assertEquals(original.consumerGroupId(), roundtrip.consumerGroupId());
        assertEquals(original.topicPartition(), roundtrip.topicPartition());
        assertEquals(original.upstreamOffset(), roundtrip.upstreamOffset());
        assertEquals(original.downstreamOffset(), roundtrip.downstreamOffset());
        assertEquals(original.metadata(), roundtrip.metadata());
    }

    @Test
    public void testDifferentPartitions() {
        TopicPartition tp1 = new TopicPartition("topic", 0);
        TopicPartition tp2 = new TopicPartition("topic", 1);
        
        Checkpoint c1 = new Checkpoint("group", tp1, 100L, 200L, "meta");
        Checkpoint c2 = new Checkpoint("group", tp2, 100L, 200L, "meta");
        
        assertNotEquals(c1, c2);
        assertNotEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    public void testSameInstance() {
        TopicPartition tp = new TopicPartition("topic", 1);
        Checkpoint c1 = new Checkpoint("group", tp, 100L, 200L, "meta");
        
        assertEquals(c1, c1);
    }
}
