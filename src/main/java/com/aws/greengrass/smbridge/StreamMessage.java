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
public class StreamMessage {
    private String stream;
    private byte[] payload;
}
