/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.smbridge;

import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor
public class Metadata {
    @Setter
    private LocalDateTime timestamp;
    @Setter
    private String topic;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        if (timestamp != null) {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSSSSS");
            String stringTime = dtf.format(timestamp);
            sb.append("\"timestamp\":\"");
            sb.append(stringTime);
            sb.append('"');
            if (topic != null) {
                sb.append(',');
            }
        }
        if (topic != null) {
            sb.append("\"topic\":\"");
            sb.append(topic);
            sb.append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    public boolean isEmpty() {
        return timestamp == null && topic == null;
    }
}
