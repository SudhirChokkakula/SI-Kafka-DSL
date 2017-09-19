package com.si.kafka.dsl.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.messaging.MessageHandler;

/**
 * 
 * @author sudhirc
 *
 */
@Configuration
public class KafkaDSLProducingChannelConfig {
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String kafkaBootstrapServers;
	
	@Value("${spring.kafka.template.default-topic}")
	private String kafkaTopic;
	
	@Value("${input.directory}")
	private File directory;
	
	@Bean
	public DirectChannel producingChannel() {
		return new DirectChannel();
	}
	
	@Bean
	public DirectChannel stringInboundChannel() {
		return new DirectChannel();
	}
	
	@Bean
	public MessageHandler kafkaMessageHandler() {
		KafkaProducerMessageHandler<String, String> messaheHandler = new KafkaProducerMessageHandler<>(kafkaTemplate());
		messaheHandler.setMessageKeyExpression(new LiteralExpression("spring-itegration"));
		messaheHandler.setTopicExpression(new LiteralExpression(kafkaTopic));
		return messaheHandler;
	}
	
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
	
	@Bean
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}
	
	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> properties = new HashMap<>();
	    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
	    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    // introduce a delay on the send to allow more messages to accumulate
	    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
	    
	    return properties;
	}
	
	@Bean
	public MessageSource<File> fileMessageSource() {
	    FileReadingMessageSource fileReadingMessageSource = new FileReadingMessageSource();
	    fileReadingMessageSource.setDirectory(directory);
	    fileReadingMessageSource.setFilter(new AcceptOnceFileListFilter<>());
	    return fileReadingMessageSource;
	}
	
	@Bean
	public FileToStringTransformer fileToStringTransformer() {
	    return new FileToStringTransformer();
	}
	
	@Bean
	public IntegrationFlow integrationFlow() {
		return IntegrationFlows.from(fileMessageSource(), c -> c.poller(Pollers.fixedRate(100).maxMessagesPerPoll(5)))
				.channel(producingChannel())
				.transform(fileToStringTransformer())
				.channel(stringInboundChannel())
				.handle(kafkaMessageHandler())
				.get();
	}

}
