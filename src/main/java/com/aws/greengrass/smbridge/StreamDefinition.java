/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import lombok.Getter;
import java.util.ArrayList;
import java.util.List;


/**
 *  Stream definitions to create new streams in SM Client
 */
public class StreamDefinition {
    @Getter
    private List<MessageStreamDefinition> streams = new ArrayList<>();

}
