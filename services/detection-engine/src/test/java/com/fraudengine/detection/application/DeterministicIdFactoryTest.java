package com.fraudengine.detection.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DeterministicIdFactoryTest {

  private final DeterministicIdFactory factory = new DeterministicIdFactory();

  @Test
  void generatesSameAssessmentIdForSameEvent() {
    String first = factory.assessmentId("event-001");

    String second = factory.assessmentId("event-001");

    assertThat(first).isEqualTo(second);
  }

  @Test
  void remainsDeterministicAcrossDifferentFactoryInstances() {
    String first = new DeterministicIdFactory().assessmentId("event-001");

    String second = new DeterministicIdFactory().assessmentId("event-001");

    assertThat(first).isEqualTo(second);
  }

  @Test
  void differentEventsProduceDifferentAssessmentIds() {
    assertThat(factory.assessmentId("event-001")).isNotEqualTo(factory.assessmentId("event-002"));
  }

  @Test
  void usesSeparateNamespacesForDifferentIdTypes() {
    String sameInput = "same-input";

    String assessment = factory.assessmentId(sameInput);

    String alert = factory.alertId(sameInput);

    String notification = factory.notificationRequestId(sameInput);

    assertThat(assessment).isNotEqualTo(alert);
    assertThat(assessment).isNotEqualTo(notification);
    assertThat(alert).isNotEqualTo(notification);
  }

  @Test
  void derivesStableAlertFromAssessment() {
    String assessmentId = factory.assessmentId("event-001");

    assertThat(factory.alertId(assessmentId)).isEqualTo(factory.alertId(assessmentId));
  }

  @Test
  void derivesStableNotificationRequestOnlyFromAlert() {
    String assessmentId = factory.assessmentId("event-001");

    String alertId = factory.alertId(assessmentId);

    assertThat(factory.notificationRequestId(alertId))
        .isEqualTo(factory.notificationRequestId(alertId));
  }

  @Test
  void rejectsNullId() {
    assertThatThrownBy(() -> factory.assessmentId(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ID_MUST_NOT_BE_BLANK");
  }

  @Test
  void rejectsEmptyId() {
    assertThatThrownBy(() -> factory.alertId(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ID_MUST_NOT_BE_BLANK");
  }

  @Test
  void rejectsBlankId() {
    assertThatThrownBy(() -> factory.notificationRequestId("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ID_MUST_NOT_BE_BLANK");
  }
}
