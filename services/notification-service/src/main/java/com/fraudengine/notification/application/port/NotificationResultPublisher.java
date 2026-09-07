package com.fraudengine.notification.application.port;

import com.fraudengine.contracts.NotificationResult;

public interface NotificationResultPublisher {

  void publish(NotificationResult result);
}
