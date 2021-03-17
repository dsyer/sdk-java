package io.cloudevents.examples.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
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
    private StreamBridge streamBridge;

    private ArrayBlockingQueue<Message<String>> queue = new ArrayBlockingQueue<>(1);

    @KafkaListener(id = "test", topics = "events-out-0", clientIdPrefix = "cloudEvents")
    public void listen(Message<String> message) {
        System.err.println(message);
        queue.add(message);
    }

    @Test
    void echoWithCorrectHeaders() throws Exception {

        streamBridge.send("events-in-0", MessageBuilder.withPayload("{\"value\":\"Dave\"}".getBytes()) //
                        .setHeader("ce-id", "12345") //
                        .setHeader("ce-specversion", "1.0") //
                        .setHeader("ce-type", "io.spring.event") //
                        .setHeader("ce-source", "https://spring.io/events") //
                        .setHeader("ce-datacontenttype", MimeType.valueOf("application/json")) //
                        .build());

        Message<String> response = queue.poll(2000, TimeUnit.MILLISECONDS);

        assertThat(response).isNotNull();
        assertThat(response.getPayload()).isEqualTo("{\"value\":\"Dave\"}");

        MessageHeaders headers = response.getHeaders();

        assertThat(headers.get("ce-id")).isNotNull();
        assertThat(headers.get("ce-source")).isNotNull();
        assertThat(headers.get("ce-type")).isNotNull();

        assertThat(headers.get("ce-id")).isNotEqualTo("12345");
        assertThat(headers.get("ce-type")).isEqualTo("io.spring.event.Foo");
        assertThat(headers.get("ce-source")).isEqualTo("https://spring.io/foos");

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
