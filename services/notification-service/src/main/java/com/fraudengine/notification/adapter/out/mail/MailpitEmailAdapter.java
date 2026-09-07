package com.fraudengine.notification.adapter.out.mail;

import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.notification.application.port.CustomerContactPort;
import com.fraudengine.notification.application.port.NotificationChannelPort;
import jakarta.mail.Message;
import jakarta.mail.internet.MimeMessage;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.springframework.mail.javamail.JavaMailSender;

public final class MailpitEmailAdapter implements NotificationChannelPort {

  private static final String CHANNEL = "EMAIL";

  private final JavaMailSender mailSender;
  private final String fromAddress;

  public MailpitEmailAdapter(
      JavaMailSender mailSender,
      String fromAddress) {
    this.mailSender = Objects.requireNonNull(mailSender);
    this.fromAddress = Objects.requireNonNull(fromAddress);
  }

  @Override
  public ChannelResult send(
      CustomerNotificationRequested request,
      CustomerContactPort.CustomerContact contact) {

    try {
      MimeMessage message = mailSender.createMimeMessage();

      message.setFrom(fromAddress);
      message.setRecipients(
          Message.RecipientType.TO,
          contact.email());

      message.setSubject(
          "Suspicious transaction detected",
          StandardCharsets.UTF_8.name());

      message.setText(
          """
          We detected activity that requires your attention.

          Please review your account through the official application.
          """,
          StandardCharsets.UTF_8.name());

      message.saveChanges();

      String providerReference =
          canonicalMessageId(message.getMessageID());

      mailSender.send(message);

      return new ChannelResult(
          CHANNEL,
          providerReference);

    } catch (Exception error) {
      throw new IllegalStateException(
          "EMAIL_DELIVERY_FAILED",
          error);
    }
  }

  private static String canonicalMessageId(String messageId) {
    if (messageId == null || messageId.isBlank()) {
      throw new IllegalStateException("EMAIL_MESSAGE_ID_NOT_GENERATED");
    }

    String normalized = messageId.trim();

    if (normalized.startsWith("<")
        && normalized.endsWith(">")
        && normalized.length() > 2) {

      return normalized.substring(
          1,
          normalized.length() - 1);
    }

    return normalized;
  }
}
