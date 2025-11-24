package org.itmo.distributed.component;

import java.util.UUID;

import org.itmo.distributed.dto.ResultMessage;
import org.itmo.distributed.dto.TaskMessage;
import org.itmo.distributed.service.TextProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@EnableRabbit
@Profile("worker")
public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final TextProcessingService processingService;
    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.exchange}")
    private String exchange;

    @Value("${app.rabbitmq.routing-key.results}")
    private String routingKey;

    public Worker(TextProcessingService processingService, RabbitTemplate rabbitTemplate) {
        this.processingService = processingService;
        this.rabbitTemplate = rabbitTemplate;
        logger.info("Initialized worker with uuid: {}", UUID.randomUUID());
    }

    @RabbitListener(queues = "${app.rabbitmq.queue.tasks}")
    public void processTask(TaskMessage task) {
        logger.info("Got task with id: {}, chunk index: {} out of: {} chunks.", task.id(), task.chunkIndex(), task.totalChunks());

        ResultMessage result = processingService.process(task);
        rabbitTemplate.convertAndSend(exchange, routingKey, result);
    }
}
