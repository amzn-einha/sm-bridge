/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasLength;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MetadataTest {
    @Test
    void GIVEN_new_metadata_THEN_is_empty() throws Exception {
        Metadata metadata = new Metadata();
        assertTrue(metadata.isEmpty());
    }

    @Test
    void GIVEN_metadata_with_some_values_THEN_generates_string() throws Exception {
        Metadata metadata = new Metadata();
        metadata.setTopic("myTopic");
        assertFalse(metadata.isEmpty());

        String json = metadata.toString();
        String expected = "{\"topic\":\"myTopic\"}";
        assertThat(json, equalTo(expected));
    }

    @Test
    void GIVEN_metadata_with_all_values_THEN_generates_string() throws Exception {
        Metadata metadata = new Metadata();
        metadata.setTopic("testTopic");
        LocalDateTime now = LocalDateTime.now();
        metadata.setTimestamp(now);

        String json = metadata.toString();
        String expected = "{\"timestamp\":\"yyyy/MM/dd HH:mm:ss.SSSSSS\",\"topic\":\"testTopic\"}";
        assertThat(json, endsWith("\",\"topic\":\"testTopic\"}"));
        assertThat(json, hasLength(expected.length()));
    }
}
