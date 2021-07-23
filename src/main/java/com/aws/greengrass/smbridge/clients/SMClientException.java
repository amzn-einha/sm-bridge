/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge.clients;

/**
 * Exception thrown by the SM Client.
 */
public class SMClientException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    SMClientException(String msg, Throwable cause) {
        super(msg, cause);
    }

    SMClientException(String msg) {
        super(msg);
    }
}
