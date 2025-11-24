package org.itmo.distributed.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record ResultMessage(
    String taskId,
    int chunkIndex,
    int totalChunks,
    long wordCount,
    Map<String, Integer> wordFrequencies,
    int positiveCount,
    int negativeCount,
    String modifiedText,
    List<String> sortedSentences
) implements Serializable {}
