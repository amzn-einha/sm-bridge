/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge.clients;

/**
 * Exception thrown by the Message Clients.
 */
public class MQTTClientException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    /**
     * Ctr for {@link MQTTClientException}.
     *
     * @param msg   message
     * @param cause cause of the exception
     */
    public MQTTClientException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Ctr for {@link MQTTClientException}.
     *
     * @param msg message
     */
    public MQTTClientException(String msg) {
        super(msg);
    }
}
