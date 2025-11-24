package org.itmo.distributed.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.itmo.distributed.dto.ResultMessage;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@Profile("aggregator")
public class Aggregator {

    private final Map<String, AggregatedData> storage = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private long startTime = 0;

    public Aggregator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "${app.rabbitmq.queue.results}")
    public void collectResult(ResultMessage result) {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        storage.compute(result.taskId(), (id, existingData) -> {
            AggregatedData data = existingData;
            if (data == null) {
                data = new AggregatedData();
                data.totalChunks = result.totalChunks();
            }
            
            data.totalWords += result.wordCount();
            data.totalPositive += result.positiveCount();
            data.totalNegative += result.negativeCount();

            data.textParts.put(result.chunkIndex(), result.modifiedText());

            data.allSentences.addAll(result.sortedSentences());
            
            final AggregatedData finalData = data;
            result.wordFrequencies().forEach((word, count) -> 
                finalData.wordFrequency.merge(word, count, Integer::sum)
            );
            
            int currentCount = data.processedChunks.incrementAndGet();
            
            if (currentCount == data.totalChunks) {
                finalizeTask(id, data);
                return null;
            }
            
            return data;
        });
    }

    private void finalizeTask(String taskId, AggregatedData data) {
        long duration = System.currentTimeMillis() - startTime;

        StringBuilder fullModifiedText = new StringBuilder();
        for (int i = 0; i < data.totalChunks; i++) {
            String part = data.textParts.get(i);
            if (part != null) {
                fullModifiedText.append(part).append(" ");
            }
        }

        List<String> globalSortedSentences = new ArrayList<>(data.allSentences);
        globalSortedSentences.sort(Comparator.comparingInt(String::length));

        Map<String, Integer> topNWords = data.wordFrequency.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(5)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        FinalReport report = new FinalReport(
                taskId,
                duration,
                data.totalWords,
                new SentimentReport(data.totalPositive, data.totalNegative),
                topNWords,
                fullModifiedText.toString().trim(),
                globalSortedSentences
        );

        try {
            String jsonOutput = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);
            System.out.println("===== Final Result JSON =====");
            System.out.println(jsonOutput);
            System.out.println("=============================");
        } catch (Exception e) {
            System.err.println("Error generating JSON report: " + e.getMessage());
        }

        startTime = 0;
    }

    private static class AggregatedData {
        int totalChunks;
        long totalWords = 0;
        long totalPositive = 0;
        long totalNegative = 0;
        final Map<String, Integer> wordFrequency = new ConcurrentHashMap<>();
        final Map<Integer, String> textParts = new ConcurrentSkipListMap<>();
        final List<String> allSentences = new CopyOnWriteArrayList<>();
        final AtomicInteger processedChunks = new AtomicInteger(0);
    }

    private record FinalReport(
        String taskId,
        long processingTimeMs,
        long totalWords,
        SentimentReport sentiment,
        Map<String, Integer> topNWords,
        String modifiedText,
        List<String> sortedSentences
    ) {}
    
    private record SentimentReport(long positiveSentences, long negativeSentences) {}
}
