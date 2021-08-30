/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 *  Stream definitions to create new streams in SM Client.
 */
@NoArgsConstructor
public class StreamDefinition {
    @Getter
    private Map<String, MessageStreamDefinition> streams = new HashMap<>();

    private List<StreamDefinition.UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    public void addEntry(String key, MessageStreamDefinition messageStreamDefinition) {
        streams.put(key, messageStreamDefinition);
    }

    public List<MessageStreamDefinition> getList() {
        return new ArrayList<>(streams.values());
    }

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the definition map to the passed argument.
     *
     * @param mapping   the key-definition mapping to be updated
     */
    public void updateDefinition(@NonNull Map<String, MessageStreamDefinition> mapping) {
        this.streams = mapping;
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }

}