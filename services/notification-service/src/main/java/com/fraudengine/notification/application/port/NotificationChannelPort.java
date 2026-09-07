package com.fraudengine.notification.application.port;

import com.fraudengine.contracts.CustomerNotificationRequested;

public interface NotificationChannelPort {

  ChannelResult send(
      CustomerNotificationRequested request, CustomerContactPort.CustomerContact contact);

  record ChannelResult(String channel, String providerReference) {}
}
