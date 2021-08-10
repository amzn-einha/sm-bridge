/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.smbridge.clients.MQTTClient;
import com.aws.greengrass.smbridge.clients.SMClient;
import com.aws.greengrass.smbridge.clients.SMClientException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Bridges/Routes the messages flowing from local MQTT to SM. This class process the topics mappings. It
 * tells the clients to subscribe to the relevant topics and routes the messages to other clients when received.
 */
public class MessageBridge {
    private static final Logger LOGGER = LogManager.getLogger(MessageBridge.class);

    private final TopicMapping topicMapping;
    private MQTTClient mqttClient;
    private SMClient smClient;

    // A map from a source topic to a Mapping Entry. The Entry specifies the output stream and the optional
    // append-values.
    // Example:
    // "/sourceTopic1" -> [{"/sourceTopic1", "outputStream1", false, true}]
    private Map<String, List<TopicMapping.MappingEntry>> sourceDestinationMap = new HashMap<>();

    /**
     * Ctr for Message Bridge.
     *
     * @param topicMapping topics mapping
     */
    public MessageBridge(TopicMapping topicMapping) {
        this.topicMapping = topicMapping;
        this.topicMapping.listenToUpdates(this::processMapping);
        processMapping();
    }

    private void handleMessage(MQTTMessage message) {
        String sourceTopic = message.getTopic();
        LOGGER.atDebug().kv("sourceTopic", sourceTopic).log("Message received");

        LOGGER.atDebug().kv("destinations", sourceDestinationMap).log("Message will be forwarded to destinations");

        if (sourceDestinationMap != null) {
            final Consumer<TopicMapping.MappingEntry> processDestination = destination -> {
                String stream = destination.getStream();
                StreamMessage streamMessage;
                byte[] payload;
                JSONObject jsonObject = new JSONObject();

                if (destination.isAppendTime()){
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSSSSS");
                    LocalDateTime now = LocalDateTime.now();
                    String stringTime = dtf.format(now);

                    jsonObject.put("timestamp", stringTime);
                }
                if (destination.isAppendTopic()){
                    jsonObject.put("topic", message.getTopic());
                }
                if (!jsonObject.isEmpty()) {
                    byte[] jsonBytes = jsonObject.toString().getBytes();
                    byte[] headerLengthInBytes = {(byte) (jsonBytes.length >> 8), (byte) jsonBytes.length};
                    payload = new byte[2 + jsonBytes.length + message.getPayload().length];

                    System.arraycopy(headerLengthInBytes, 0, payload, 0, 2);
                    System.arraycopy(jsonBytes, 0, payload, 2, jsonBytes.length);
                    System.arraycopy(
                            message.getPayload(), 0, payload, 2 + jsonBytes.length, message.getPayload().length);
                } else {
                    payload = message.getPayload();
                }
                streamMessage = new StreamMessage(stream, payload);
                try {
                    smClient.publish(streamMessage);
                    LOGGER.atInfo().kv("Source Topic", message.getTopic()).kv("Destination Stream", stream)
                            .log("Published message");
                } catch (SMClientException e) {
                    LOGGER.atError().setCause(e).kv("Stream", stream).log("Stream Publish failed");
                }
            };
            // Perform topic matching on filter
            sourceDestinationMap.entrySet().stream().filter(
                    mapping -> MqttTopic.isMatched(mapping.getKey(), sourceTopic))
                    .map(Map.Entry::getValue)
                    .forEach(perTopicMapping -> perTopicMapping.forEach(processDestination));
        }
    }

    private void processMapping() {
        List<TopicMapping.MappingEntry> mappingEntryList = topicMapping.getList();
        LOGGER.atDebug().kv("topicMapping", mappingEntryList).log("Processing mapping");

        Map<String, List<TopicMapping.MappingEntry>> sourceDestinationMapTemp = new HashMap<>();

        mappingEntryList.forEach(mappingEntry -> {
            // Add destinations for each source topic
            sourceDestinationMapTemp.computeIfAbsent(mappingEntry.getTopic(), k -> new ArrayList<>())
                    .add(mappingEntry);
        });

        sourceDestinationMap = sourceDestinationMapTemp;

        if (mqttClient != null) {
            updateSubscriptionsForClient(mqttClient);
        }
        LOGGER.atDebug().kv("topicMapping", sourceDestinationMap).log("Processed mapping");
    }

    public void addOrReplaceMqttClient(MQTTClient mqttClient){
        this.mqttClient = mqttClient;
        updateSubscriptionsForClient(mqttClient);
    }

    private synchronized void updateSubscriptionsForClient(MQTTClient mqttClient) {
        Set<String> topicsToSubscribe;
        if (sourceDestinationMap == null) {
            topicsToSubscribe = new HashSet<>();
        } else {
            topicsToSubscribe = sourceDestinationMap.keySet();
        }

        LOGGER.atDebug().kv("topics", topicsToSubscribe).log("Updating subscriptions");

        mqttClient.updateSubscriptions(topicsToSubscribe, message -> handleMessage(message));
    }
}
