package com.fraudengine.notification.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;

class NotificationConfigurationTest {

  @Test
  void stopsListenerContainerWhenNotificationProcessingFails() {
    var configuration = new NotificationConfiguration();

    assertThat(configuration.notificationKafkaErrorHandler())
        .isInstanceOf(CommonContainerStoppingErrorHandler.class);
  }
}
