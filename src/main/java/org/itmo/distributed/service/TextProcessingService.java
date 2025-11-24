package org.itmo.distributed.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.itmo.distributed.dto.ResultMessage;
import org.itmo.distributed.dto.TaskMessage;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("worker")
public class TextProcessingService {
    private static final String NAMED_ENTITY_TOKEN = "PERSON";
    private static final String NAMED_ENTITY_REPLACEMENT = "[NAME]";

    private static final int NEGATIVE_SENTIMENT = 1;
    private static final int POSITIVE_SENTIMENT = 3;

    private final StanfordCoreNLP nlp;

    public TextProcessingService() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, sentiment");
        this.nlp = new StanfordCoreNLP(props);
    }

    public ResultMessage process(TaskMessage task) {
        String text = task.content();
        if (text == null || text.isBlank()) {
            return new ResultMessage(task.id(), task.chunkIndex(), task.totalChunks(), 0, Map.of(), 0, 0, "", List.of());
        }

        Annotation document = new Annotation(text);
        nlp.annotate(document);

        List<CoreLabel> tokens = document.get(CoreAnnotations.TokensAnnotation.class);

        long wordCount = 0;
        Map<String, Integer> wordFrequencies = new HashMap<>();

        for (CoreLabel token : tokens) {
            String word = token.word();
            if (isWord(word)) {
                wordCount++;
                wordFrequencies.merge(word.toLowerCase(), 1, Integer::sum);
            }
        }

        SentimentStats sentiment = calculateSentiment(document);

        String modifiedText = replaceNames(text, tokens);

        List<String> sortedSentences = sortSentences(document);

        return new ResultMessage(
                task.id(),
                task.chunkIndex(),
                task.totalChunks(),
                wordCount,
                wordFrequencies,
                sentiment.positive,
                sentiment.negative,
                modifiedText,
                sortedSentences
        );
    }

    private boolean isWord(String token) {
        for (char c : token.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                return true;
            }
        }
        return false;
    }

    private SentimentStats calculateSentiment(Annotation document) {
        int positive = 0;
        int negative = 0;

        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int score = RNNCoreAnnotations.getPredictedClass(tree);

            if (score <= NEGATIVE_SENTIMENT) {
                negative++;
            } else if (score >= POSITIVE_SENTIMENT) {
                positive++;
            }
        }
        return new SentimentStats(positive, negative);
    }

    private String replaceNames(String originalText, List<CoreLabel> tokens) {
        StringBuilder sb = new StringBuilder();
        int previousEnd = 0;

        for (CoreLabel token : tokens) {
            String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
            int start = token.beginPosition();
            int end = token.endPosition();

            sb.append(originalText, previousEnd, start);

            if (NAMED_ENTITY_TOKEN.equals(ne)) {
                sb.append(NAMED_ENTITY_REPLACEMENT);
            } else {
                sb.append(originalText, start, end);
            }
            previousEnd = end;
        }

        sb.append(originalText.substring(previousEnd));
        return sb.toString();
    }

    private List<String> sortSentences(Annotation document) {
        List<String> sentences = new ArrayList<>();
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            sentences.add(sentence.toString());
        }
        sentences.sort(Comparator.comparingInt(String::length));
        return sentences;
    }

    private record SentimentStats(int positive, int negative) {
    }
}
