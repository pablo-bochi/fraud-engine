package com.fraudengine.notification.adapter.out.mail;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.notification.application.port.CustomerContactPort;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class MailpitEmailAdapterIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Container
  static final GenericContainer<?> MAILPIT =
      new GenericContainer<>(DockerImageName.parse("axllent/mailpit:v1.31.0"))
          .withExposedPorts(1025, 8025);

  @Test
  void sendsEmailThroughMailpit() throws Exception {
    JavaMailSenderImpl mailSender = new JavaMailSenderImpl();

    mailSender.setHost(MAILPIT.getHost());
    mailSender.setPort(MAILPIT.getMappedPort(1025));

    var adapter = new MailpitEmailAdapter(mailSender, "fraud-engine@example.test");

    var result =
        adapter.send(
            request(), new CustomerContactPort.CustomerContact("customer-123@example.test"));

    assertThat(result.channel()).isEqualTo("EMAIL");
    assertThat(result.providerReference()).isNotBlank();

    JsonNode messages = awaitMessages(1);

    assertThat(messages.path("total").asInt()).isEqualTo(1);

    JsonNode message = messages.path("messages").get(0);

    assertThat(message.path("To").get(0).path("Address").asText())
        .isEqualTo("customer-123@example.test");

    assertThat(message.path("MessageID").asText()).isNotBlank();
  }

  private static JsonNode awaitMessages(int expected) throws Exception {
    HttpClient client = HttpClient.newHttpClient();

    URI uri =
        URI.create(
            "http://" + MAILPIT.getHost() + ":" + MAILPIT.getMappedPort(8025) + "/api/v1/messages");

    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();

    while (System.nanoTime() < deadline) {
      HttpRequest request =
          HttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(2)).build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).isEqualTo(200);

      JsonNode body = MAPPER.readTree(response.body());

      if (body.path("total").asInt() >= expected) {
        return body;
      }

      Thread.sleep(50);
    }

    throw new AssertionError("EXPECTED_" + expected + "_MAILPIT_MESSAGES");
  }

  private static CustomerNotificationRequested request() {
    return new CustomerNotificationRequested(
        1,
        "notification-123",
        "alert-123",
        "customer-123",
        "transaction-123",
        "SUSPICIOUS_TRANSACTION",
        "suspicious-transaction-v1",
        "pt-BR",
        Instant.parse("2026-09-06T17:59:00Z"),
        "trace-123");
  }
}
