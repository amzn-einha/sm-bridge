/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.amazonaws.greengrass.streammanager.client.exception.InvalidRequestException;
import com.amazonaws.greengrass.streammanager.client.utils.ValidateAndSerialize;
import com.amazonaws.greengrass.streammanager.model.S3ExportTaskDefinition;
import com.amazonaws.greengrass.streammanager.model.sitewise.AssetPropertyValue;
import com.amazonaws.greengrass.streammanager.model.sitewise.PutAssetPropertyValueEntry;
import com.amazonaws.greengrass.streammanager.model.sitewise.TimeInNanos;
import com.amazonaws.greengrass.streammanager.model.sitewise.Variant;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.smbridge.clients.MQTTClient;
import com.aws.greengrass.smbridge.clients.SMClient;
import com.aws.greengrass.smbridge.clients.SMClientException;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

    public void addOrReplaceMqttClient(MQTTClient mqttClient) {
        this.mqttClient = mqttClient;
        updateSubscriptionsForClient(mqttClient);
    }

    public void addOrReplaceSMClient(SMClient smClient) {
        this.smClient = smClient;
    }

    private byte[] prepareS3Payload(TopicMapping.MappingEntry destination, byte[] message)
            throws IOException, InvalidRequestException {
        String filepath = "/tmp/sm-bridge/" + Utils.generateRandomString(7) + ".txt";
        File file = new File(filepath);
        Files.write(file.toPath(), message);
        URI uri = file.toURI();
        S3ExportTaskDefinition s3ExportTaskDefinition = new S3ExportTaskDefinition()
                .withBucket(destination.getS3Export().getS3Bucket())
                .withKey(destination.getS3Export().getS3key() + '/' + file.getName())
                .withInputUrl(uri.getPath());
        return ValidateAndSerialize.validateAndSerializeToJsonBytes(s3ExportTaskDefinition);
    }

    private byte[] prepareSiteWisePayload(TopicMapping.MappingEntry destination, byte[] message)
            throws InvalidRequestException, JsonProcessingException {
        List<AssetPropertyValue> entries = new ArrayList<>();
        TimeInNanos timestamp = new TimeInNanos().withTimeInSeconds(Instant.now().getEpochSecond());
        AssetPropertyValue entry = new AssetPropertyValue().withTimestamp(timestamp)
                .withValue(new Variant().withStringValue(new String(message, StandardCharsets.UTF_8)));
        entries.add(entry);
        PutAssetPropertyValueEntry putAssetPropertyValueEntry = new PutAssetPropertyValueEntry()
                .withEntryId(UUID.randomUUID().toString())
                .withPropertyAlias(destination.getSiteWisePropertyAlias())
                .withPropertyValues(entries);
        return ValidateAndSerialize.validateAndSerializeToJsonBytes(putAssetPropertyValueEntry);
    }

    private byte[] preparePayload(TopicMapping.MappingEntry destination, MQTTMessage message)
            throws InvalidRequestException, IOException {
        byte[] payload;
        JSONObject jsonObject = new JSONObject();

        if (destination.isAppendTime()) {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSSSSS");
            LocalDateTime now = LocalDateTime.now();
            String stringTime = dtf.format(now);

            jsonObject.put("timestamp", stringTime);
        }
        if (destination.isAppendTopic()) {
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
        if (destination.getS3Export() != null && StringUtils.isNotBlank(destination.getS3Export().getS3Bucket())
                && StringUtils.isNotBlank(destination.getS3Export().getS3key())) {
            try {
                return prepareS3Payload(destination, payload);
            } catch (InvalidRequestException | IOException e) {
                LOGGER.atError().setCause(e).log("Unable to prepare S3 export");
                throw e;
            }
        } else if (StringUtils.isNotBlank(destination.getSiteWisePropertyAlias())) {
            try {
                return prepareSiteWisePayload(destination, payload);
            } catch (InvalidRequestException | JsonProcessingException e) {
                LOGGER.atError().setCause(e).log("Unable to prepare SiteWise export");
                throw e;
            }
        } else {
            return payload;
        }
    }

    private void handleMessage(MQTTMessage message) {
        String sourceTopic = message.getTopic();
        LOGGER.atDebug().kv("sourceTopic", sourceTopic).log("Message received");

        LOGGER.atDebug().kv("destinations", sourceDestinationMap).log("Message will be forwarded to destinations");

        if (sourceDestinationMap != null) {
            final Consumer<TopicMapping.MappingEntry> processDestination = destination -> {
                String stream = destination.getStream();
                LOGGER.atDebug().kv("stream", stream).kv("topic", message.getTopic()).log("Forwarding message");
                try {
                    StreamMessage streamMessage = new StreamMessage(stream, preparePayload(destination, message));
                    smClient.publish(streamMessage);
                    LOGGER.atInfo().kv("Source Topic", message.getTopic()).kv("Destination Stream", stream)
                            .log("Published message");
                } catch (InvalidRequestException | IOException | SMClientException e) {
                    LOGGER.atError().setCause(e).kv("Stream", stream).log("Could not export payload");
                }
            };
            // Perform topic matching on filter from mapped topics/destinations
            sourceDestinationMap.entrySet().stream().filter(
                    mapping -> MqttTopic.isMatched(mapping.getKey(), sourceTopic))
                    .map(Map.Entry::getValue)
                    .forEach(perTopicMapping -> perTopicMapping.forEach(processDestination));

            // Perform topic matching against reserved topic
            if (MqttTopic.isMatched(SMBridge.RESERVED_TOPIC, sourceTopic)) {
                String stream = sourceTopic.split("/")[1];
                try {
                    StreamMessage streamMessage = new StreamMessage(stream, preparePayload(
                        new TopicMapping.MappingEntry(
                                "", "", SMBridge.APPEND_TIME_DEFAULT_STREAM, SMBridge.APPEND_TOPIC_DEFAULT_STREAM), message));

                    smClient.publish(streamMessage);
                    LOGGER.atInfo().kv("Source Topic", message.getTopic()).kv("Destination Stream", stream)
                            .log("Published message");
                } catch (InvalidRequestException | IOException | SMClientException e) {
                    LOGGER.atError().setCause(e).kv("Stream", stream).log("Could not export payload");
                }
            }
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

    private synchronized void updateSubscriptionsForClient(MQTTClient mqttClient) {
        Set<String> topicsToSubscribe;
        if (sourceDestinationMap == null) {
            topicsToSubscribe = new HashSet<>();
        } else {
            topicsToSubscribe = new HashSet<>(sourceDestinationMap.keySet());
        }
        topicsToSubscribe.add(SMBridge.RESERVED_TOPIC);

        LOGGER.atDebug().kv("topics", topicsToSubscribe).log("Updating subscriptions");

        mqttClient.updateSubscriptions(topicsToSubscribe, message -> handleMessage(message));
    }
}
