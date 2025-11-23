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

import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorClientConfigAdditionalTest {

    @Test
    public void replicationPolicyIsConfiguredFromConfigProps() {
        Map<String, Object> props = new HashMap<>();
        // Required minimal client config
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Custom separator and disabling for internal topics
        props.put(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR, "__");
        props.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, false);

        MirrorClientConfig config = new MirrorClientConfig(props);

        ReplicationPolicy policy = config.replicationPolicy();
        assertNotNull(policy);
        assertTrue(policy instanceof DefaultReplicationPolicy);

        // Separator should be applied for normal topics
        assertEquals("us__orders", policy.formatRemoteTopic("us", "orders"));
        assertEquals("us", policy.topicSource("us__orders"));
        assertEquals("orders", policy.upstreamTopic("us__orders"));

        // When internal separator is disabled, internal topics should use "."
        assertEquals("CLUSTER.checkpoints.internal", policy.checkpointsTopic("CLUSTER"));
        assertEquals("mm2-offset-syncs.CLUSTER.internal", policy.offsetSyncsTopic("CLUSTER"));
    }

    @Test
    public void clientSubConfigsAreFilteredAndScoped() {
        Map<String, Object> props = new HashMap<>();
        // Global required property
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "ignored:9092");

        // Admin sub-configs
        props.put("admin." + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "a:9092,b:9092");
        props.put("admin.security.protocol", "PLAINTEXT");
        props.put("admin.foobar", "should-be-stripped");

        // Consumer sub-configs
        props.put("consumer." + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "c:9092");
        props.put("consumer.security.protocol", "PLAINTEXT");
        props.put("consumer.some.unrelated", "should-be-stripped");

        MirrorClientConfig config = new MirrorClientConfig(props);

        Map<String, Object> admin = config.adminConfig();
        assertTrue(admin.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), "admin.bootstrap.servers kept");
        assertTrue(admin.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), "admin.security.protocol kept");
        assertFalse(admin.containsKey("foobar"), "arbitrary admin key stripped");

        // Ensure the values match the sub-configs, not the global one
        assertTrue(admin.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString().contains("a:9092"),
            "admin bootstrap.servers should reflect admin.* value");

        Map<String, Object> consumer = config.consumerConfig();
        assertTrue(consumer.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), "consumer.bootstrap.servers kept");
        assertTrue(consumer.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), "consumer.security.protocol kept");
        assertFalse(consumer.containsKey("some.unrelated"), "arbitrary consumer key stripped");
        assertTrue(consumer.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString().contains("c:9092"),
            "consumer bootstrap.servers should reflect consumer.* value");
    }
}
