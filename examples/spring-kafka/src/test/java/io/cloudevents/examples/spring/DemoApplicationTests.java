package io.cloudevents.examples.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeType;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(initializers = DemoApplicationTests.Initializer.class)
public class DemoApplicationTests {

    @Autowired
    private KafkaTemplate<byte[], byte[]> kafka;

    @Autowired
    private KafkaListenerConfiguration listener;

    @Test
    void echoWithCorrectHeaders() throws Exception {

        kafka.send(MessageBuilder.withPayload("{\"value\":\"Dave\"}".getBytes()) //
                        .setHeader(KafkaHeaders.TOPIC, "events-in-0") //
                        .setHeader("ce_id", "12345") //
                        .setHeader("ce_specversion", "1.0") //
                        .setHeader("ce_type", "io.spring.event") //
                        .setHeader("ce_source", "https://spring.io/events") //
                        .setHeader("ce_datacontenttype", MimeType.valueOf("application/json")) //
                        .build());

        Message<String> response = listener.queue.poll(2000, TimeUnit.MILLISECONDS);

        assertThat(response).isNotNull();
        assertThat(response.getPayload()).isEqualTo("{\"value\":\"Dave\"}");

        MessageHeaders headers = response.getHeaders();

        assertThat(headers.get("ce_id")).isNotNull();
        assertThat(headers.get("ce_source")).isNotNull();
        assertThat(headers.get("ce_type")).isNotNull();

        assertThat(headers.get("ce_id")).isNotEqualTo("12345");
        assertThat(headers.get("ce_type")).isEqualTo("io.spring.event.Foo");
        assertThat(headers.get("ce_source")).isEqualTo("https://spring.io/foos");

    }

    @TestConfiguration
    static class KafkaListenerConfiguration {

        private ArrayBlockingQueue<Message<String>> queue = new ArrayBlockingQueue<>(1);

        @KafkaListener(id = "test", topics = "events-out-0", clientIdPrefix = "cloudEvents")
        public void listen(Message<String> message) {
            System.err.println(message);
            queue.add(message);
        }

    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        private static KafkaContainer kafka;

        static {
            kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("5.4.3")) //
                    .withNetwork(null); // .withReuse(true);
            kafka.start();
        }

        @Override
        public void initialize(ConfigurableApplicationContext context) {
            TestPropertyValues.of("spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers()).applyTo(context);
        }

    }
}
