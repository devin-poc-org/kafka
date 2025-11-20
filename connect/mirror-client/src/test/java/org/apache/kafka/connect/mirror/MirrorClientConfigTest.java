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
import org.apache.kafka.clients.admin.ForwardingAdmin;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorClientConfigTest {

    @Test
    public void testConfigConstruction() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        assertNotNull(config);
    }

    @Test
    public void testReplicationPolicy() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        ReplicationPolicy policy = config.replicationPolicy();
        
        assertNotNull(policy);
        assertTrue(policy instanceof DefaultReplicationPolicy);
    }

    @Test
    public void testCustomReplicationPolicy() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(MirrorClientConfig.REPLICATION_POLICY_CLASS, IdentityReplicationPolicy.class.getName());
        props.put(IdentityReplicationPolicy.SOURCE_CLUSTER_ALIAS_CONFIG, "source");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        ReplicationPolicy policy = config.replicationPolicy();
        
        assertNotNull(policy);
        assertTrue(policy instanceof IdentityReplicationPolicy);
    }

    @Test
    public void testAdminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        Map<String, Object> adminConfig = config.adminConfig();
        
        assertNotNull(adminConfig);
        assertEquals(List.of("localhost:9092"), adminConfig.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        Map<String, Object> consumerConfig = config.consumerConfig();
        
        assertNotNull(consumerConfig);
        assertEquals(List.of("localhost:9092"), consumerConfig.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        Map<String, Object> producerConfig = config.producerConfig();
        
        assertNotNull(producerConfig);
        assertEquals(List.of("localhost:9092"), producerConfig.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testForwardingAdmin() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        Map<String, Object> adminConfig = config.adminConfig();
        ForwardingAdmin admin = config.forwardingAdmin(adminConfig);
        
        assertNotNull(admin);
        admin.close();
    }

    @Test
    public void testConfigWithCustomSeparator() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR, "__");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        assertEquals("__", config.getString(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR));
    }

    @Test
    public void testConfigWithInternalTopicSeparatorDisabled() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED, "false");
        
        MirrorClientConfig config = new MirrorClientConfig(props);
        assertFalse(config.getBoolean(MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED));
    }
}
