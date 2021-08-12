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
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.smbridge.StreamDefinition;
import com.aws.greengrass.smbridge.StreamMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;

import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class SMClientTest {

    @Mock
    private Topics mockTopics;

    @Mock
    private StreamDefinition mockStreamDefinition;

    @Mock
    private StreamManagerClient mockSmClient;

    @Test
    void WHEN_call_sm_client_constructed_THEN_does_not_throw() {
        new SMClient(mockTopics, mockStreamDefinition, mockSmClient);
    }

    @Test
    void GIVEN_sm_client_WHEN_publish_AND_stream_exists_THEN_publish() throws Exception {
        when(mockSmClient.describeMessageStream("mqttToStreamDefaultStreamName")).thenReturn(new MessageStreamInfo());

        SMClient smClient = new SMClient(mockTopics, mockStreamDefinition, mockSmClient);
        smClient.start();
        smClient.publish(new StreamMessage("mqttToStreamDefaultStreamName", "Message".getBytes()));
        verify(mockSmClient, times(1)).describeMessageStream("mqttToStreamDefaultStreamName");
        verify(mockSmClient, times(0)).updateMessageStream(any(MessageStreamDefinition.class));
        verify(mockSmClient, times(1)).appendMessage(any(String.class), any());
    }

    @Test
    void GIVEN_sm_client_WHEN_publish_AND_stream_doesnt_exist_AND_not_configured_THEN_create_default_stream() throws Exception {
        doThrow(new StreamManagerException("TestExceptionCause")).when(mockSmClient).describeMessageStream("testStream");

        SMClient smClient = new SMClient(mockTopics, mockStreamDefinition, mockSmClient);
        smClient.start();
        smClient.publish(new StreamMessage("testStream", "Message".getBytes()));
        verify(mockSmClient, times(1)).describeMessageStream("testStream");
        verify(mockSmClient, times(1)).createMessageStream(any(MessageStreamDefinition.class));
        verify(mockSmClient, times(1)).appendMessage(any(String.class), any());
    }

    @Test
    void GIVEN_sm_client_WHEN_publish_AND_stream_doesnt_exist_AND_configured_THEN_create_stream() throws Exception {
        MessageStreamDefinition msd = new MessageStreamDefinition(
                "testStream", 268435456L, 16777216L, 9223372036854L,
                StrategyOnFull.RejectNewData, Persistence.File, false, null);
        when(mockStreamDefinition.getList()).thenReturn(new ArrayList<>(Collections.singleton(msd)));
        doThrow(new StreamManagerException("TestExceptionCause")).when(mockSmClient).describeMessageStream("testStream");

        SMClient smClient = new SMClient(mockTopics, mockStreamDefinition, mockSmClient);
        smClient.start();
        smClient.publish(new StreamMessage("testStream", "Message".getBytes()));
        verify(mockSmClient, times(1)).describeMessageStream("testStream");
        verify(mockSmClient, times(1)).createMessageStream(msd);
        verify(mockSmClient, times(1)).appendMessage(any(String.class), any());
    }

    @Test
    void GIVEN_sm_client_WHEN_start_throws_exception_THEN_sm_client_exception_is_thrown() throws Exception {
        doThrow(new StreamManagerException("TestExceptionCause")).when(mockSmClient).describeMessageStream(any());
        doThrow(new StreamManagerException("TestExceptionCause")).when(mockSmClient).createMessageStream(any());
        SMClient smClient = new SMClient(mockTopics, mockStreamDefinition, mockSmClient);
        smClient.start();
        Assertions.assertThrows(SMClientException.class, () -> {
            smClient.publish(new StreamMessage("RandomStream", "HelloWorld".getBytes()));
        });
    }
}
