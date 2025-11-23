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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class CheckpointRoundTripTest {

    @Test
    public void serializeDeserializeRoundTrip() {
        TopicPartition tp = new TopicPartition("test-topic", 3);
        Checkpoint cp = new Checkpoint("group-1", tp, 10L, 20L, "md");

        byte[] key = cp.recordKey();
        byte[] value = cp.recordValue();

        ConsumerRecord<byte[], byte[]> record =
            new ConsumerRecord<>("checkpoints.internal", 0, 0L, key, value);

        Checkpoint parsed = Checkpoint.deserializeRecord(record);

        assertEquals("group-1", parsed.consumerGroupId());
        assertEquals(tp, parsed.topicPartition());
        assertEquals(10L, parsed.upstreamOffset());
        assertEquals(20L, parsed.downstreamOffset());
        assertEquals("md", parsed.metadata());

        OffsetAndMetadata oam = parsed.offsetAndMetadata();
        assertEquals(20L, oam.offset());
        assertEquals("md", oam.metadata());
    }

    @Test
    public void connectPartitionAndUnwrapGroup() {
        TopicPartition tp = new TopicPartition("orders", 1);
        Checkpoint cp = new Checkpoint("g", tp, 1L, 2L, "");

        Map<String, ?> part = cp.connectPartition();
        assertEquals("g", part.get(Checkpoint.CONSUMER_GROUP_ID_KEY));
        assertEquals("orders", part.get(Checkpoint.TOPIC_KEY));
        assertEquals(1, part.get(Checkpoint.PARTITION_KEY));

        assertEquals("g", Checkpoint.unwrapGroup(part));
    }

    @Test
    public void equalsAndHashCode() {
        TopicPartition tp1 = new TopicPartition("t", 0);
        Checkpoint a = new Checkpoint("g", tp1, 5L, 6L, "m");
        Checkpoint b = new Checkpoint("g", tp1, 5L, 6L, "m");
        TopicPartition tp2 = new TopicPartition("t", 1);
        Checkpoint c = new Checkpoint("g", tp2, 5L, 6L, "m");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
