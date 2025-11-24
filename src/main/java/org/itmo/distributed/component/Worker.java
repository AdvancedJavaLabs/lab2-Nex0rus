package org.itmo.distributed.component;

import java.util.UUID;

import org.itmo.distributed.dto.ResultMessage;
import org.itmo.distributed.dto.TaskMessage;
import org.itmo.distributed.service.TextProcessingService;
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

    private final TextProcessingService processingService;
    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.exchange}")
    private String exchange;

    @Value("${app.rabbitmq.routing-key.results}")
    private String routingKey;

    public Worker(TextProcessingService processingService, RabbitTemplate rabbitTemplate) {
        this.processingService = processingService;
        this.rabbitTemplate = rabbitTemplate;
        System.out.println("Initialized worker with uuid: " + UUID.randomUUID());
    }

    @RabbitListener(queues = "${app.rabbitmq.queue.tasks}")
    public void processTask(TaskMessage task) {
        System.out.println(
                "Got task with id: " + task.id() +
                "chunk index: " + task.chunkIndex() +
                "out of: " + task.totalChunks() + "chunks."
        );

        ResultMessage result = processingService.process(task);
        rabbitTemplate.convertAndSend(exchange, routingKey, result);
    }
}
