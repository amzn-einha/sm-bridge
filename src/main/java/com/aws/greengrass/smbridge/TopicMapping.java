/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.amazonaws.greengrass.streammanager.model.S3ExportTaskDefinition;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Topic mappings from mqtt topic to other topics (iot core or pub sub).
 */
@NoArgsConstructor
public class TopicMapping {
    // Map from arbitrariy, unique mapping key to mapping entry. 
    // Each entry contains MQTT->Stream configuration
    @Getter
    private Map<String, MappingEntry> mapping = new HashMap<>();

    private List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    public List<MappingEntry> getList() {
        return new ArrayList<>(mapping.values());
    }

    /**
     * A single entry in the mapping.
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class MappingEntry {
        @Getter
        @JsonProperty("topic")
        private String topic;
        @Getter
        @JsonProperty("stream")
        private String stream;
        @Getter
        @JsonProperty("appendTime")
        private boolean appendTime = false;
        @Getter
        @JsonProperty("appendTopic")
        private boolean appendTopic = false;
        @Getter
        @JsonProperty("s3Export")
        private S3Export s3Export;
        @Getter
        @JsonProperty("siteWisePropertyAlias")
        private String siteWisePropertyAlias;

        public MappingEntry(String topic, String stream, boolean appendTime, boolean appendTopic) {
            this.topic = topic;
            this.stream = stream;
            this.appendTime = appendTime;
            this.appendTopic = appendTopic;
        }

        @Override
        public String toString() {
            return String.format(
                    "{topic: %s, stream: %s, appendTime: %b, appendTopic: %b}",
                    topic, stream, appendTime, appendTopic
            );
        }

        @AllArgsConstructor
        @NoArgsConstructor
        @EqualsAndHashCode
        public static class S3Export {
            @Getter
            @JsonProperty("s3Bucket")
            private String s3Bucket;
            @Getter
            @JsonProperty("s3Key")
            private String s3key;
        }

    }

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the topic mapping to the passed argument.
     *
     * @param mapping       mapping from entry key to mapping entry 
     */
    public void updateMapping(@NonNull Map<String, MappingEntry> mapping) {
        // TODO: Check for duplicates, General validation + unit tests. Topic strings need to be validated (allowed
        //  filter?, etc)
        this.mapping = mapping;
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }
}
