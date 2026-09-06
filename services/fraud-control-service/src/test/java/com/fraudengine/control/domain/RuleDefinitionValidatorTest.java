package com.fraudengine.control.domain;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class RuleDefinitionValidatorTest {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final RuleDefinitionValidator validator = new RuleDefinitionValidator(8, 900);

  @Test
  void acceptsAnAmountThresholdWithinTheDeclaredBounds() throws Exception {
    assertThatCode(
            () ->
                validator.validate(
                    objectMapper.readTree(
                        """
        {"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}
        """)))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsExecutableExpressions() throws Exception {
    assertThatThrownBy(
            () ->
                validator.validate(
                    objectMapper.readTree(
                        """
        {"type":"SPEL","expression":"T(java.lang.Runtime).getRuntime()"}
        """)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("UNSUPPORTED_RULE_NODE");
  }

  @Test
  void rejectsAWindowBeyondTheConfiguredHistoricalLimit() throws Exception {
    assertThatThrownBy(
            () ->
                validator.validate(
                    objectMapper.readTree(
                        """
        {"type":"COUNT_WINDOW","windowSeconds":901,"minimumCount":2}
        """)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("WINDOW_EXCEEDS_LIMIT");
  }

  @Test
  void rejectsAttributeComparisonBecauseItIsNotExecutableInTheMvp() throws Exception {
    assertThatThrownBy(
            () ->
                validator.validate(
                    objectMapper.readTree(
                        """
                        {
                          "type":"ATTRIBUTE_COMPARISON",
                          "attribute":"merchantCategory",
                          "operator":"EQ",
                          "value":"travel"
                        }
                        """)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("UNSUPPORTED_RULE_NODE");
  }

  @Test
  void rejectsAnOversizedCompositeRule() throws Exception {
    assertThatThrownBy(
            () ->
                validator.validate(
                    objectMapper.readTree(
                        """
                        {"type":"ALL","children":[
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"},
                        {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL"}]}
                        """)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("COMPOSITE_WIDTH_EXCEEDS_LIMIT");
  }

  @Test
  void rejectsSumWindowBecauseItIsNotExecutableInTheMvp() throws Exception {
    assertThatThrownBy(
            () ->
                validator.validate(
                    objectMapper.readTree(
                        """
                        {
                          "type":"SUM_WINDOW",
                          "windowSeconds":600,
                          "minimumAmountMinor":10000
                        }
                        """)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("UNSUPPORTED_RULE_NODE");
  }
}
