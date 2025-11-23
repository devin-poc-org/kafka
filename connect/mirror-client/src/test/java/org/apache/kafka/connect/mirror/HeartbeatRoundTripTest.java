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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HeartbeatRoundTripTest {

    @Test
    public void serializeDeserializeRoundTrip() {
        Heartbeat hb = new Heartbeat("sourceA", "targetB", 123456789L);

        byte[] key = hb.recordKey();
        byte[] value = hb.recordValue();

        ConsumerRecord<byte[], byte[]> record =
            new ConsumerRecord<>("heartbeats", 0, 0L, key, value);

        Heartbeat parsed = Heartbeat.deserializeRecord(record);

        assertEquals("sourceA", parsed.sourceClusterAlias());
        assertEquals("targetB", parsed.targetClusterAlias());
        assertEquals(123456789L, parsed.timestamp());
    }

    @Test
    public void connectPartitionContainsSourceAndTarget() {
        Heartbeat hb = new Heartbeat("primary", "backup", 42L);
        Map<String, ?> partition = hb.connectPartition();
        assertEquals("primary", partition.get(Heartbeat.SOURCE_CLUSTER_ALIAS_KEY));
        assertEquals("backup", partition.get(Heartbeat.TARGET_CLUSTER_ALIAS_KEY));
    }
}
