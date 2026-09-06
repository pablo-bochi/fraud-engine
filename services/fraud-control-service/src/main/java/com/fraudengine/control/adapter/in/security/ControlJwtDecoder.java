package com.fraudengine.control.adapter.in.security;

import java.security.interfaces.RSAPublicKey;
import java.util.List;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

public final class ControlJwtDecoder {
  private ControlJwtDecoder() {}

  public static NimbusJwtDecoder create(RSAPublicKey publicKey, String issuer, String audience) {
    NimbusJwtDecoder decoder = NimbusJwtDecoder.withPublicKey(publicKey).build();
    decoder.setJwtValidator(
        new DelegatingOAuth2TokenValidator<>(
            JwtValidators.createDefaultWithIssuer(issuer),
            new JwtClaimValidator<List<String>>(
                "aud", value -> value != null && value.contains(audience)),
            new JwtClaimValidator<String>("sub", value -> value != null && !value.isBlank()),
            new JwtClaimValidator<java.time.Instant>("exp", java.util.Objects::nonNull)));
    return decoder;
  }
}
