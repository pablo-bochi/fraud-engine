package com.fraudengine.notification.adapter.out.contact;

import com.fraudengine.notification.application.port.CustomerContactPort;
import java.util.Objects;

public final class FixtureCustomerContactAdapter implements CustomerContactPort {

  private final String email;

  public FixtureCustomerContactAdapter(String email) {
    this.email = Objects.requireNonNull(email);

    if (email.isBlank()) {
      throw new IllegalArgumentException("FIXTURE_EMAIL_MUST_NOT_BE_BLANK");
    }
  }

  @Override
  public CustomerContact findByCustomerId(String customerId) {
    if (customerId == null || customerId.isBlank()) {
      throw new IllegalArgumentException("CUSTOMER_ID_MUST_NOT_BE_BLANK");
    }

    return new CustomerContact(email);
  }
}
