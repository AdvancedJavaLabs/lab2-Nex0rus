package org.itmo.distributed.component;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.itmo.distributed.dto.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

@Component
@Profile("producer")
public class Producer implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final int SENTENCES_PER_TASK = 50;

    private final RabbitTemplate rabbitTemplate;
    private final ApplicationArguments applicationArguments;
    private final StanfordCoreNLP pipeline;

    @Value("${app.rabbitmq.exchange}")
    private String exchange;
    
    @Value("${app.rabbitmq.routing-key.tasks}")
    private String routingKey;

    public Producer(RabbitTemplate rabbitTemplate, ApplicationArguments applicationArguments) {
        this.rabbitTemplate = rabbitTemplate;
        this.applicationArguments = applicationArguments;

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit");
        this.pipeline = new StanfordCoreNLP(props);

        logger.info("Initialized producer with uuid: {}", UUID.randomUUID());
    }

    @Override
    public void run(String... args) throws Exception {
        List<String> sourceArgs = applicationArguments.getOptionValues("source");

        String fullText;
        
        if (sourceArgs != null && !sourceArgs.isEmpty()) {
            String sourcePath = sourceArgs.getFirst();
            logger.info("Reading text from: {}", sourcePath);
            fullText = readText(sourcePath);
        } else {
            logger.warn("No source provided (use --source=<path>).");
            return;
        }

        if (fullText.isEmpty()) {
            logger.warn("Text is empty. Exiting.");
            return;
        }

        logger.info("Splitting text into sentences...");
        List<String> sentences = splitIntoSentences(fullText);
        logger.info("Found {} sentences.", sentences.size());

        List<String> chunks = new ArrayList<>();
        StringBuilder currentChunk = new StringBuilder();
        
        for (int i = 0; i < sentences.size(); i++) {
            currentChunk.append(sentences.get(i)).append(" ");
            
            if ((i + 1) % SENTENCES_PER_TASK == 0) {
                chunks.add(currentChunk.toString().trim());
                currentChunk.setLength(0);
            }
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk.toString().trim());
        }

        String taskId = UUID.randomUUID().toString();
        int total = chunks.size();
        
        long startTime = System.currentTimeMillis();
        logger.info("Sending {} tasks (chunks of ~{} sentences) for TaskID: {}", total, SENTENCES_PER_TASK, taskId);

        for (int i = 0; i < total; i++) {
            TaskMessage msg = new TaskMessage(taskId, i, total, chunks.get(i));
            rabbitTemplate.convertAndSend(exchange, routingKey, msg);
        }
        
        logger.info("All tasks sent in {}ms", System.currentTimeMillis() - startTime);
    }

    private List<String> splitIntoSentences(String text) {
        List<String> sentences = new ArrayList<>();
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            sentences.add(sentence.toString());
        }
        return sentences;
    }

    private String readText(String sourcePath) throws IOException {
        Path path = Paths.get(sourcePath);
        StringBuilder sb = new StringBuilder();

        if (Files.isDirectory(path)) {
            try (Stream<Path> stream = Files.walk(path)) {
                stream.filter(Files::isRegularFile)
                      .filter(p -> p.toString().endsWith(".txt"))
                      .forEach(p -> {
                          try {
                              sb.append(Files.readString(p)).append(" ");
                          } catch (IOException e) {
                              logger.error("Error reading file: {}", p, e);
                          }
                      });
            }
        } else if (Files.isRegularFile(path)) {
            sb.append(Files.readString(path));
        } else {
            logger.error("Invalid source path: {}", sourcePath);
        }
        return sb.toString();
    }
}
