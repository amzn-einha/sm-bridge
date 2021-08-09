/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge.clients;

import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.amazonaws.greengrass.streammanager.model.MessageStreamInfo;
import com.amazonaws.greengrass.streammanager.model.Persistence;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.smbridge.StreamDefinition;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import org.mockito.stubbing.OngoingStubbing;

import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class SMClientTest {

    private static final String DEFAULT_STREAM = "mqttToStreamDefaultStreamName";

    @Mock
    private Topics mockTopics;

    @Mock
    private MessageStreamInfo mockMsi;

    @Mock
    private StreamManagerClient mockSmClient;

    @Test
    void WHEN_call_sm_client_constructed_THEN_does_not_throw() {
        new SMClient(mockTopics, mockSmClient);
    }

    @Test
    void GIVEN_sm_client_WHEN_call_start_AND_stream_exists_THEN_update_stream() throws Exception {
        MessageStreamDefinition msd = new MessageStreamDefinition(
                "mqttToStreamDefaultStreamName", 268435456L, 16777216L, 9223372036854L,
                StrategyOnFull.RejectNewData, Persistence.File, false, null);

        when(mockSmClient.listStreams()).thenReturn(new ArrayList<String>(Collections.singleton("mqttToStreamDefaultStreamName")));
        when(mockMsi.getDefinition()).thenReturn(msd);
        when(mockSmClient.describeMessageStream(msd.getName())).thenReturn(mockMsi);

        SMClient smClient = new SMClient(mockTopics, mockSmClient);
        smClient.start();
        verify(mockSmClient, times(1)).listStreams();
        verify(mockSmClient, times(1)).updateMessageStream(any(MessageStreamDefinition.class));
    }

    @Test
    void GIVEN_sm_client_WHEN_call_start_AND_stream_doesnt_exist_THEN_create_stream() throws Exception {
        MessageStreamDefinition msd = new MessageStreamDefinition(
                "mqttToStreamDefaultStreamName", 268435456L, 16777216L, 9223372036854L,
                StrategyOnFull.RejectNewData, Persistence.File, false, null);

        when(mockSmClient.listStreams()).thenReturn(new ArrayList<String>(Collections.singleton("randomName")));

        SMClient smClient = new SMClient(mockTopics, mockSmClient);
        smClient.start();
        verify(mockSmClient, times(1)).listStreams();
        verify(mockSmClient, times(1)).createMessageStream(any(MessageStreamDefinition.class));
    }

    @Test
    void GIVEN_sm_client_WHEN_start_throws_exception_THEN_sm_client_exception_is_thrown() throws Exception {
        doThrow(new StreamManagerException("TestExceptionCause")).when(mockSmClient).listStreams();
        SMClient smClient = new SMClient(mockTopics, mockSmClient);
        Assertions.assertThrows(SMClientException.class, smClient::start);
    }
}
