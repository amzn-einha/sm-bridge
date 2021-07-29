/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge.clients;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.amazonaws.greengrass.streammanager.model.MessageStreamInfo;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.amazonaws.greengrass.streammanager.model.export.ExportDefinition;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.smbridge.StreamMessage;
import com.aws.greengrass.smbridge.StreamDefinition;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;

import javax.inject.Inject;
import java.util.List;

public class SMClient {
    private static final Logger LOGGER = LogManager.getLogger(SMClient.class);

    private StreamManagerClient smClient;
    private MessageStreamDefinition defaultStream;
    /**
     * Ctr for SMClient.
     *
     * @param topics             topics passed in by Nucleus
     * @throws SMClientException if unable to create SM Client
     */
    @Inject
    public SMClient(Topics topics) throws SMClientException {
        this(topics, null);
        // TODO: Handle the case when serverUri is modified
        try {
            this.smClient = StreamManagerClientFactory.standard().build();
        } catch (StreamManagerException e) {
            throw new SMClientException("Unable to create a SM client", e);
        }
    }
    protected SMClient(Topics topics, StreamManagerClient streamManagerClient){
        this.smClient = streamManagerClient;
        // TODO: Configurable default stream
        this.defaultStream = new MessageStreamDefinition();
        this.defaultStream.setName("mqttToStreamDefaultStreamName");
        this.defaultStream.setTimeToLiveMillis(9223372036854L);
        this.defaultStream.setStrategyOnFull(StrategyOnFull.RejectNewData);
    }

    // TODO: Connect to stream manager on custom port
    //  https://docs.aws.amazon.com/greengrass/v2/developerguide/use-stream-manager-in-custom-components.html
    public void start() throws SMClientException {
        try {
            updateOrCreateStream(defaultStream);
        } catch (StreamManagerException e) {
            LOGGER.atError().log("Encountered StreamManagerException while starting SM Client");
            throw new SMClientException("Unable to start SMClient");
        }
    }

    public void publishOnDefaultStream(byte[] payload) throws SMClientException{
        // TODO: Configurable default stream name
        String defaultStreamName = "mqttToStreamDefaultStreamName";
        publish(new StreamMessage(defaultStreamName, payload));
    }

    public void publish(StreamMessage message) throws SMClientException{
        try {
            if (!checkStreamExists(message.getStream())) {
                // TODO: Configurable default stream
                MessageStreamDefinition newStream = new MessageStreamDefinition();
                newStream.setName(message.getStream());
                newStream.setTimeToLiveMillis(9223372036854L);
                newStream.setStrategyOnFull(StrategyOnFull.RejectNewData);
                createStream(newStream);
            }
        } catch (StreamManagerException e) {
            LOGGER.atWarn().kv("Stream", message.getStream()).setCause(e).log("Unable to create stream");
            return;
        }

        try {
            long sequenceNumber = smClient.appendMessage(message.getStream(), message.getPayload());
        } catch (StreamManagerException e) {
            LOGGER.atWarn().kv("Stream", message.getStream()).setCause(e).log("Unable to append to stream");
        }
    }

    private void updateOrCreateStream(MessageStreamDefinition msd) throws StreamManagerException {
        if (checkStreamExists(msd.getName())){
            updateStream(msd);
        } else {
            createStream(msd);
        }
    }

    private boolean checkStreamExists(String stream) throws StreamManagerException {
        List<String> streams = smClient.listStreams();
        return streams.contains(stream);
    }

    private void updateStream(MessageStreamDefinition msd) throws StreamManagerException {
        MessageStreamInfo messageStreamInfo;
        messageStreamInfo = smClient.describeMessageStream(msd.getName());

        smClient.updateMessageStream(
                messageStreamInfo.getDefinition()
                        .withMaxSize(msd.getMaxSize())
                        .withStreamSegmentSize(msd.getStreamSegmentSize())
                        .withTimeToLiveMillis(msd.getTimeToLiveMillis())
                        .withStrategyOnFull(msd.getStrategyOnFull())
                        .withPersistence(msd.getPersistence())
                        .withFlushOnWrite(msd.getFlushOnWrite())
                        .withExportDefinition(null)

        );
    }

    private void createStream(MessageStreamDefinition msd) throws StreamManagerException {
        smClient.createMessageStream(msd);
    }
}
