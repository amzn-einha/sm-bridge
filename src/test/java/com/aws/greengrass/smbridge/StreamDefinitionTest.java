/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.amazonaws.greengrass.streammanager.model.Persistence;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class StreamDefinitionTest {

    @Test
    void GIVEN_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_updated_successfully() throws Exception {
        StreamDefinition streamDefinition = new StreamDefinition();
        CountDownLatch updateLatch = new CountDownLatch(1);
        streamDefinition.listenToUpdates(updateLatch::countDown);

        Map<String, MessageStreamDefinition> streamsToUpdate = Utils.immutableMap(
                "m1", new MessageStreamDefinition(),
                "m2", new MessageStreamDefinition("1", 2L, 3L, 4L, StrategyOnFull.OverwriteOldestData, Persistence.File, false, null),
                "m3", new MessageStreamDefinition("2", 3L, 4L, 5L, StrategyOnFull.RejectNewData, Persistence.Memory, false, null));
        streamDefinition.updateDefinition(streamsToUpdate);

        Assertions.assertTrue(updateLatch.await(100, TimeUnit.MILLISECONDS));

        Map<String, MessageStreamDefinition> expectedMapping = new HashMap<>();
        expectedMapping.put("m1", new MessageStreamDefinition());
        expectedMapping.put("m2", new MessageStreamDefinition("1", 2L, 3L, 4L, StrategyOnFull.OverwriteOldestData, Persistence.File, false, null));
        expectedMapping.put("m3", new MessageStreamDefinition("2", 3L, 4L, 5L, StrategyOnFull.RejectNewData, Persistence.Memory, false, null));

        assertEquals(streamDefinition.getStreams(), expectedMapping);
    }

    @Test
    void GIVEN_null_mapping_as_json_string_WHEN_updateMapping_THEN_NPE_thrown() throws Exception {
        StreamDefinition streamDefinition = new StreamDefinition();
        assertThat(streamDefinition.getStreams().size(), is(equalTo(0)));
        Assertions.assertThrows(NullPointerException.class, () -> streamDefinition.updateDefinition(null));
        assertThat(streamDefinition.getStreams().size(), is(equalTo(0)));
    }
}