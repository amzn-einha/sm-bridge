/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 *  Stream definitions to create new streams in SM Client
 */
public class StreamDefinition {
    @Getter
    private Map<String, StreamDefinitionEntry> streams = new HashMap<>();

    public ArrayList<MessageStreamDefinition> getList() {
        return new ArrayList<>(streams.values())
    }

    ;

    /**
     * * A single message stream definition (wrapper), where all values are primitive
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class StreamDefinitionEntry {
        @Getter
        @JsonProperty("name")
        private String name = "mqttToStreamDefaultStreamName";
        @Getter
        @JsonProperty("maxSize")
        private long maxSize = 268435456L;
        @Getter
        @JsonProperty("streamSegmentSize")
        private long streamSegmentSize = 16777216L;
        @Getter
        @JsonProperty("ttl")
        private long ttl = 9223372036854L;
        @Getter
        @JsonProperty("strategyOnFull")
        private String strategyOnFull = "OverwriteOldestData";
        @Getter
        @JsonProperty("persistence")
        private String persistence = "File";
        @Getter
        @JsonProperty("flushOnWrite")
        private boolean flushOnWrite = false;
        @Getter
        @JsonProperty("exportDefinition")
        private exportDefinitionEntry exportDefinition = new exportDefinitionEntry();

        @Override
        public String toString() {
            return String.format(
                    "{name: %s, maxSiz: %d, segmentSize: %d, ttl: %d, strategyOnFull: %s, persistence: %s, " +
                            "flushOnWrite: %b, exportDefinition: %s}", name, maxSize, streamSegmentSize, ttl,
                    strategyOnFull, persistence, flushOnWrite, exportDefinition
            );
        }
    }


    public static class exportDefinitionEntry {

    }

    public void updateDefinition(@NonNull Map<String, StreamDefinitionEntry> mapping) {
        this.streams = mapping;
    }

}

/*
    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the topic mapping to the passed argument
     *
     * @param mapping       mapping to update
     */
  /*  public void updateMapping(@NonNull Map<String, MappingEntry> mapping) {
        // TODO: Check for duplicates, General validation + unit tests. Topic strings need to be validated (allowed
        //  filter?, etc)
        this.mapping = mapping;
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }
}
*/