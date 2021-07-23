/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import lombok.Value;

/**
 * Common representation of a Message.
 */
@Value
public class MQTTMessage {
    private String topic;
    private byte[] payload;
}
