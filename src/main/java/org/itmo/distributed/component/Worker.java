package org.itmo.distributed.component;

import org.itmo.distributed.dto.ResultMessage;
import org.itmo.distributed.dto.TaskMessage;
import org.itmo.distributed.service.TextProcessingService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
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
    }

    @RabbitListener(queues = "${app.rabbitmq.queue.tasks}")
    public void processTask(TaskMessage task) {
        ResultMessage result = processingService.process(task);
        rabbitTemplate.convertAndSend(exchange, routingKey, result);
    }
}
