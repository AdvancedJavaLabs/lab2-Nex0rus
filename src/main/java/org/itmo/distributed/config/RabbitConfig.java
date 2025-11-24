package org.itmo.distributed.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    @Value("${app.rabbitmq.exchange}")
    private String exchangeName;

    @Value("${app.rabbitmq.queue.tasks}")
    private String taskQueueName;

    @Value("${app.rabbitmq.queue.results}")
    private String resultQueueName;

    @Value("${app.rabbitmq.routing-key.tasks}")
    private String taskRoutingKey;

    @Value("${app.rabbitmq.routing-key.results}")
    private String resultRoutingKey;

    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(exchangeName);
    }

    @Bean
    public Queue taskQueue() {
        return new Queue(taskQueueName, false);
    }

    @Bean
    public Queue resultQueue() {
        return new Queue(resultQueueName, false);
    }

    @Bean
    public Binding taskBinding(DirectExchange exchange, Queue taskQueue) {
        return BindingBuilder.bind(taskQueue).to(exchange).with(taskRoutingKey);
    }

    @Bean
    public Binding resultBinding(DirectExchange exchange, Queue resultQueue) {
        return BindingBuilder.bind(resultQueue).to(exchange).with(resultRoutingKey);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        return template;
    }
}


