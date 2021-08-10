/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge.clients;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
import com.amazonaws.greengrass.streammanager.client.config.StreamManagerClientConfig;
import com.amazonaws.greengrass.streammanager.client.config.StreamManagerServerInfo;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.smbridge.StreamDefinition;
import com.aws.greengrass.smbridge.StreamMessage;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClient;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationRequest;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationResponse;
import software.amazon.awssdk.eventstreamrpc.EventStreamRPCConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;

public class SMClient {
    private static final Logger LOGGER = LogManager.getLogger(SMClient.class);

    private StreamManagerClient streamManagerClient;
    private StreamDefinition streamDefinition;
    private MessageStreamDefinition defaultStreamDefinition;

    /**
     * Ctr for SMClient.
     *
     * @param  topics            topics passed in by Nucleus
     * @param  streamDefinition  stream configs as parsed by SMBridge class
     * @throws SMClientException if unable to create SM Client
     */
    @Inject
    public SMClient(Topics topics, StreamDefinition streamDefinition) throws SMClientException {
        this(topics, streamDefinition, null);
        // TODO: Handle the case when serverUri is modified
        try {
            connectToStreamManagerWithCustomPort();
        } catch (StreamManagerException e) {
            throw new SMClientException("Unable to create a SM client", e);
        }
    }

    @SuppressWarnings("PMD.UnusedFormalParameter") // topics may be needed later for extensibility with kernel interact
    protected SMClient(Topics topics, StreamDefinition streamDefinition, StreamManagerClient streamManagerClient) {
        this.streamManagerClient = streamManagerClient;
        this.streamDefinition = streamDefinition;
    }

    @SuppressWarnings("PMD.CloseResource") // The socket is closed with a .disconnect() that pmd doesn't parse
    void connectToStreamManagerWithCustomPort() throws StreamManagerException {
        EventStreamRPCConnection eventStreamRpcConnection = null;
        try {
            eventStreamRpcConnection = IPCUtils.getEventStreamRpcConnection();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.atError("Unable to create an RPC connection from IPCUtils");
            throw new StreamManagerException("RPC/IPC Exception", e);
        }
        GreengrassCoreIPCClient greengrassCoreIPCClient = new GreengrassCoreIPCClient(eventStreamRpcConnection);
        List<String> keyPath = new ArrayList<>();
        keyPath.add("port");

        GetConfigurationRequest request = new GetConfigurationRequest();
        request.setComponentName("aws.greengrass.smbridge");
        request.setKeyPath(keyPath);
        GetConfigurationResponse response = null;
        try {
            response = greengrassCoreIPCClient.getConfiguration(request, Optional.empty()).getResponse().get();
        } catch (InterruptedException | ExecutionException e) {
            eventStreamRpcConnection.disconnect();
            LOGGER.atError("Unable to get configuration from IPC client");
            throw new StreamManagerException("IPC Client Exception", e);
        }
        eventStreamRpcConnection.disconnect();
        String port = response.getValue().get("port").toString();
        LOGGER.atInfo("Stream Manager is running on port: " + port);

        final StreamManagerClientConfig config = StreamManagerClientConfig.builder()
                .serverInfo(StreamManagerServerInfo.builder().port(Integer.parseInt(port)).build()).build();

        this.streamManagerClient = StreamManagerClientFactory.standard().withClientConfig(config).build();
    }

    /**
     *  Called after instantiation to set the default stream configuration.
     */
    public void start() {
        for (String key : streamDefinition.getStreams().keySet()) {
            if ("default".equalsIgnoreCase(key)) {
                defaultStreamDefinition = streamDefinition.getStreams().get(key);
                LOGGER.atInfo("Set default stream configuration");
                return;
            }
        }
        defaultStreamDefinition = new MessageStreamDefinition();
    }

    private MessageStreamDefinition findStreamDefinition(String streamName) {
        for (MessageStreamDefinition messageStreamDefinition : streamDefinition.getList()) {
            if (messageStreamDefinition.getName() == streamName) {
                return messageStreamDefinition;
            }
        }
        return null;
    }

    /**
     * Called by the Message Bridge message handler to publish a message to a given stream.
     *
     * @param message             encapsulates a stream name and byte-wise payload
     * @throws SMClientException  thrown if encounters an error at any point
     */
    public void publish(StreamMessage message) throws SMClientException {
        try {
            if (!checkStreamExists(message.getStream())) {
                MessageStreamDefinition newStream = findStreamDefinition(message.getStream());
                if (newStream == null) {
                    newStream = new MessageStreamDefinition(
                            message.getStream(),
                            defaultStreamDefinition.getMaxSize(),
                            defaultStreamDefinition.getStreamSegmentSize(),
                            defaultStreamDefinition.getTimeToLiveMillis(),
                            defaultStreamDefinition.getStrategyOnFull(),
                            defaultStreamDefinition.getPersistence(),
                            defaultStreamDefinition.getFlushOnWrite(),
                            defaultStreamDefinition.getExportDefinition()
                    );
                }
                streamManagerClient.createMessageStream(newStream);
            }
        } catch (StreamManagerException e) {
            LOGGER.atWarn().kv("Stream", message.getStream()).log("Unable to create stream");
            throw new SMClientException(e.getMessage(), e);
        }

        try {
            streamManagerClient.appendMessage(message.getStream(), message.getPayload());
        } catch (StreamManagerException e) {
            LOGGER.atWarn().kv("Stream", message.getStream()).log("Unable to append to stream");
            throw new SMClientException(e.getMessage(), e);
        }
    }

    private boolean checkStreamExists(String stream) {
        try {
            // Return type: MessageStreamInfo
            streamManagerClient.describeMessageStream(stream);
        } catch (StreamManagerException e) {
            return false;
        }
        return true;
    }
}
