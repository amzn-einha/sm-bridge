/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.aws.greengrass.smbridge.clients.MQTTClient;
import com.aws.greengrass.smbridge.clients.SMClient;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Utils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MessageBridgeTest {
    @Mock
    private MQTTClient mockMqttClient;
    @Mock
    private SMClient mockSmClient;
    @Mock
    private TopicMapping mockTopicMapping;

    @Test
    void WHEN_call_message_bridge_constructor_THEN_does_not_throw() {
        new MessageBridge(mockTopicMapping);
        verify(mockTopicMapping, times(1)).listenToUpdates(any());
    }

    @Test
    void GIVEN_sm_bridge_and_mapping_populated_WHEN_add_client_THEN_subscribed() throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("mqtt/topic", "RandomStream", true, false),
                "m2", new TopicMapping.MappingEntry("mqtt/topic2", "RandomStream2", false, true),
                "m3", new TopicMapping.MappingEntry("mqtt/topic3", "RandomStream2", false, false));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping);
        messageBridge.addOrReplaceMqttClient(mockMqttClient);
        ArgumentCaptor<Set<String>> topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(3));
        MatcherAssert
                .assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder(
                        "mqtt/topic", "mqtt/topic2", "mqtt/topic3"));
    }

    @Test
    void GIVEN_sm_bridge_and_mqtt_client_WHEN_mapping_populated_THEN_subscribed() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMqttClient(mockMqttClient);

        reset(mockMqttClient);
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("mqtt/topic", "RandomStream", true, false),
                "m2", new TopicMapping.MappingEntry("mqtt/topic2", "RandomStream2", false, true),
                "m3", new TopicMapping.MappingEntry("mqtt/topic3", "RandomStream2", false, false),
                "m4", new TopicMapping.MappingEntry("mqtt/topic4", "RandomStream3", false, false),
                "m5", new TopicMapping.MappingEntry("mqtt/+/topic", "RandomStream4", true, true),
                "m6", new TopicMapping.MappingEntry("mqtt/topic/#", "RandomStream5", false, false));
        mapping.updateMapping(mappingToUpdate);

        ArgumentCaptor<Set<String>> topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(6));
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(),
                Matchers.containsInAnyOrder(
                        "mqtt/topic", "mqtt/topic2", "mqtt/topic/#", "mqtt/topic3", "mqtt/topic4", "mqtt/+/topic"));
    }

    @Test
    void GIVEN_sm_bridge_with_mapping_WHEN_mapping_updated_THEN_subscriptions_updated() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMqttClient(mockMqttClient);

        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("mqtt/topic", "RandomStream", true, false),
                "m2", new TopicMapping.MappingEntry("mqtt/topic2", "RandomStream2", false, true),
                "m3", new TopicMapping.MappingEntry("mqtt/topic3", "RandomStream2", false, false),
                "m4", new TopicMapping.MappingEntry("mqtt/topic4", "RandomStream4", true, false));
        mapping.updateMapping(mappingToUpdate);

        reset(mockMqttClient);

        // Change topic 2
        // Add a new topic 3
        // Modify old topic 3
        // Remove topic 4
        mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("mqtt/topic", "RandomStream", true, false),
                "m2", new TopicMapping.MappingEntry("mqtt/topic2/changed", "RandomStream2", false, true),
                "m3", new TopicMapping.MappingEntry("mqtt/topic3/added", "RandomStream2new", false, true),
                "m4", new TopicMapping.MappingEntry("mqtt/topic3", "RandomStream2", false, false));
        mapping.updateMapping(mappingToUpdate);

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(4));
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/added", "mqtt/topic3"));

        // Remove client
        reset(mockMqttClient);
        mapping.updateMapping(Collections.EMPTY_MAP);
        topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(0));

        }

    @Test
    void GIVEN_sm_bridge_and_mapping_populated_WHEN_receive_mqtt_message_THEN_routed_to_sm() throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("mqtt/topic", "RandomStream", false, false),
                "m2", new TopicMapping.MappingEntry("mqtt/topic2", "RandomStream2", false, false),
                "m3", new TopicMapping.MappingEntry("mqtt/topic3", "RandomStream2", false, false),
                "m4", new TopicMapping.MappingEntry("mqtt/topic4", "RandomStream4", true, false));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMqttClient(mockMqttClient);
        messageBridge.addOrReplaceSMClient(mockSmClient);

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());

        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
        messageHandlerLocalMqttCaptor.getValue().accept(new MQTTMessage("mqtt/topic", messageOnTopic1));
        messageHandlerLocalMqttCaptor.getValue().accept(new MQTTMessage("mqtt/topic2", messageOnTopic2));

        // Also send on an unknown topic
        messageHandlerLocalMqttCaptor.getValue().accept(new MQTTMessage("mqtt/unknown", messageOnTopic2));

        ArgumentCaptor<StreamMessage> messageSmCaptor = ArgumentCaptor.forClass(StreamMessage.class);
        verify(mockSmClient, times(2)).publish(messageSmCaptor.capture());

        MatcherAssert.assertThat(messageSmCaptor.getAllValues().get(0).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream")));
        Assertions.assertArrayEquals(messageOnTopic1, messageSmCaptor.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messageSmCaptor.getAllValues().get(1).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream2")));
        Assertions.assertArrayEquals(messageOnTopic2, messageSmCaptor.getAllValues().get(1).getPayload());
    }

    @Test
    void GIVEN_sm_bridge_and_mapping_with_appends_WHEN_receive_mqtt_message_THEN_metadata_appended() throws Exception{
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("mqtt/topic", "RandomStream", true, false),
                "m2", new TopicMapping.MappingEntry("mqtt/topic2", "RandomStream2", false, true),
                "m3", new TopicMapping.MappingEntry("mqtt/topic3", "RandomStream2", true, true),
                "m4", new TopicMapping.MappingEntry("mqtt/topic4", "RandomStream4", true, false));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMqttClient(mockMqttClient);
        messageBridge.addOrReplaceSMClient(mockSmClient);

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());

        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
        String GenericMessageOnTopic1 = "yyyy/MM/dd HH:mm:ss.SSSSSS: message from topic mqtt/topic";
        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
        String GenericMessageOnTopic2 = "mqtt/topic2: message from topic mqtt/topic2";
        byte[] messageOnTopic3 = "message from topic mqtt/topic3".getBytes();
        String GenericMessageOnTopic3 = "yyyy/MM/dd HH:mm:ss.SSSSSS: mqtt/topic3: message from topic mqtt/topic3";

        messageHandlerLocalMqttCaptor.getValue().accept(new MQTTMessage("mqtt/topic", messageOnTopic1));
        messageHandlerLocalMqttCaptor.getValue().accept(new MQTTMessage("mqtt/topic2", messageOnTopic2));
        messageHandlerLocalMqttCaptor.getValue().accept(new MQTTMessage("mqtt/topic3", messageOnTopic3));

        ArgumentCaptor<StreamMessage> messageSmCaptor = ArgumentCaptor.forClass(StreamMessage.class);
        verify(mockSmClient, times(3)).publish(messageSmCaptor.capture());

        MatcherAssert.assertThat(messageSmCaptor.getAllValues().get(0).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream")));
        Assertions.assertEquals(
                GenericMessageOnTopic1.length(), messageSmCaptor.getAllValues().get(0).getPayload().length);

        MatcherAssert.assertThat(messageSmCaptor.getAllValues().get(1).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream2")));
        Assertions.assertArrayEquals(
                GenericMessageOnTopic2.getBytes(), messageSmCaptor.getAllValues().get(1).getPayload());

        MatcherAssert.assertThat(messageSmCaptor.getAllValues().get(2).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream2")));
        Assertions.assertEquals(
                GenericMessageOnTopic3.length(), messageSmCaptor.getAllValues().get(2).getPayload().length);
    }

    @Test
    void GIVEN_sm_bridge_and_mapping_populated_with_filters_WHEN_receive_mqtt_message_THEN_routed_correctly()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry("sensors/+/humidity", "RandomStream", false, false),
                "m2", new TopicMapping.MappingEntry("sensors/satellite/#", "RandomStream2", false, false),
                "m3", new TopicMapping.MappingEntry("sensors/satellite/altitude", "RandomStream2", false, false),
                "m4", new TopicMapping.MappingEntry("sensors/thermostat1/humidity", "RandomStream4", false, false),
                "m5", new TopicMapping.MappingEntry("sensors/thermostat1/#", "RandomStream5", false, false),
                "m6", new TopicMapping.MappingEntry("sensors/+/humidity", "RandomStream6", false, false));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping);

        messageBridge.addOrReplaceMqttClient(mockMqttClient);
        messageBridge.addOrReplaceSMClient(mockSmClient);

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMqttClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());

        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
        byte[] messageFromThermostat2 = "humidity = 41%".getBytes();
        byte[] messageFromThermostat2Temp = "temperature = 70C".getBytes();
        // Will publish 4 times
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new MQTTMessage("sensors/thermostat1/humidity", messageFromThermostat1));
        // Will publish 2 times
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new MQTTMessage("sensors/thermostat2/humidity", messageFromThermostat2));

        // Will publish one time
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new MQTTMessage("sensors/thermostat1/temperature", messageFromThermostat2Temp));
        // Also send for a topic with multiple nodes to match with the filter (which should not match)
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new MQTTMessage("sensors/thermostat2/zone1/humidity", messageFromThermostat2));

        ArgumentCaptor<StreamMessage> messageCaptor = ArgumentCaptor.forClass(StreamMessage.class);
        verify(mockSmClient, times(7)).publish(messageCaptor.capture());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(0).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream4")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageCaptor.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(1).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageCaptor.getAllValues().get(1).getPayload());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(2).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream6")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageCaptor.getAllValues().get(2).getPayload());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(3).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream5")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageCaptor.getAllValues().get(3).getPayload());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(4).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream")));
        Assertions.assertArrayEquals(messageFromThermostat2, messageCaptor.getAllValues().get(4).getPayload());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(5).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream6")));
        Assertions.assertArrayEquals(messageFromThermostat2, messageCaptor.getAllValues().get(5).getPayload());

        MatcherAssert.assertThat(messageCaptor.getAllValues().get(6).getStream(),
                Matchers.is(Matchers.equalTo("RandomStream5")));
        Assertions.assertArrayEquals(messageFromThermostat2Temp, messageCaptor.getAllValues().get(6).getPayload());
    }
}
