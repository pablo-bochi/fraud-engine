package com.fraudengine.notification.config;

import com.fraudengine.notification.adapter.out.contact.FixtureCustomerContactAdapter;
import com.fraudengine.notification.adapter.out.mail.MailpitEmailAdapter;
import com.fraudengine.notification.adapter.out.persistence.JdbcNotificationDeliveryAdapter;
import com.fraudengine.notification.application.NotificationHandler;
import com.fraudengine.notification.application.port.CustomerContactPort;
import com.fraudengine.notification.application.port.NotificationChannelPort;
import com.fraudengine.notification.application.port.NotificationDeliveryPort;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.javamail.JavaMailSender;

@Configuration(proxyBeanMethods = false)
public class NotificationConfiguration {

  @Bean
  @ConditionalOnMissingBean(NotificationDeliveryPort.class)
  @ConditionalOnProperty(
      name = "notification.persistence.enabled",
      havingValue = "true",
      matchIfMissing = true)
  NotificationDeliveryPort notificationDeliveryPort(JdbcTemplate jdbc) {
    return new JdbcNotificationDeliveryAdapter(jdbc);
  }

  @Bean
  @ConditionalOnMissingBean(CustomerContactPort.class)
  @ConditionalOnProperty(
      name = "notification.contact.fixture.enabled",
      havingValue = "true",
      matchIfMissing = true)
  CustomerContactPort fixtureCustomerContactPort(
      @Value("${notification.contact.fixture-email:customer@example.test}")
          String fixtureEmail) {

    return new FixtureCustomerContactAdapter(fixtureEmail);
  }

  @Bean
  @ConditionalOnMissingBean(NotificationChannelPort.class)
  @ConditionalOnProperty(
      name = "notification.mail.enabled",
      havingValue = "true",
      matchIfMissing = true)
  NotificationChannelPort mailpitNotificationChannelPort(
      JavaMailSender mailSender,
      @Value("${notification.mail.from-address:fraud-engine@example.test}")
          String fromAddress) {

    return new MailpitEmailAdapter(
        mailSender,
        fromAddress);
  }

  @Bean
  @ConditionalOnMissingBean(Clock.class)
  Clock notificationClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean(NotificationHandler.class)
  NotificationHandler notificationHandler(
      NotificationDeliveryPort deliveryPort,
      CustomerContactPort customerContactPort,
      NotificationChannelPort notificationChannelPort,
      Clock clock) {

    return new NotificationHandler(
        deliveryPort,
        customerContactPort,
        notificationChannelPort,
        clock);
  }
}
