package com.fraudengine.notification.application.port;

public interface CustomerContactPort {

  CustomerContact findByCustomerId(String customerId);

  record CustomerContact(String email) {}
}
