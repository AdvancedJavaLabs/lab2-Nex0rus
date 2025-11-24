package org.itmo.distributed.dto;

import java.io.Serializable;

public record TaskMessage(
    String id,
    int chunkIndex,
    int totalChunks,
    String content
) implements Serializable {
}


