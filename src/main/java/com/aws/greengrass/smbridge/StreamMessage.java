/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Common representation of a Message.
 */
@AllArgsConstructor
@Getter
public class StreamMessage {
    private String stream;
    private byte[] payload;
}
