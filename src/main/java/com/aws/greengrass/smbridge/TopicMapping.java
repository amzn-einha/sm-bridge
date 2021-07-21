/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.aws.greengrass.util.SerializerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Topic mappings from mqtt topic to other topics (iot core or pub sub).
 */
@NoArgsConstructor
public class TopicMapping {
    @Getter
    private List<MappingEntry> mapping = new ArrayList<>();

    private List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

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
    }

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the topic mapping by parsing the mapping given as json.
     *
     * @param mappingAsJson mapping as a json string
     * @throws IOException if unable to parse the string
     */
    public void updateMapping(@NonNull String mappingAsJson) throws IOException {
        final TypeReference<ArrayList<MappingEntry>> typeRef = new TypeReference<ArrayList<MappingEntry>>() {
        };
        mapping = SerializerFactory.getFailSafeJsonObjectMapper().readValue(mappingAsJson, typeRef);
        // TODO: Check for duplicates, General validation + unit tests. Topic strings need to be validated (allowed
        //  filter?, etc)
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }
}
