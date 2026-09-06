package com.fraudengine.control.adapter.in.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;

class ControlJwtDecoderTest {
  private final KeyPair keys = generateKeys();

  @Test
  void rejectsAnUnsignedToken() throws Exception {
    String signed = token("fraud-local", "fraud-control", "author", Instant.now().plusSeconds(300));
    String unsigned =
        new com.nimbusds.jwt.PlainJWT(SignedJWT.parse(signed).getJWTClaimsSet()).serialize();
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
                    .decode(unsigned))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void rejectsASignatureFromAnUntrustedKey() throws Exception {
    String token = token("fraud-local", "fraud-control", "author", Instant.now().plusSeconds(300));
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) generateKeys().getPublic(), "fraud-local", "fraud-control")
                    .decode(token))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void rejectsAnExpiredToken() throws Exception {
    String token = token("fraud-local", "fraud-control", "author", Instant.now().minusSeconds(300));
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
                    .decode(token))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void rejectsATokenWithoutExpiration() throws Exception {
    String token = token("fraud-local", "fraud-control", "author", null);
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
                    .decode(token))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void rejectsATokenWithoutAnAdministrativeSubject() throws Exception {
    String token = token("fraud-local", "fraud-control", " ", Instant.now().plusSeconds(300));
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
                    .decode(token))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void rejectsATokenIntendedForAnotherService() throws Exception {
    String token =
        token("fraud-local", "another-service", "rule-author", Instant.now().plusSeconds(300));
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
                    .decode(token))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void rejectsATokenFromAnotherIssuer() throws Exception {
    String token =
        token("untrusted", "fraud-control", "rule-author", Instant.now().plusSeconds(300));
    assertThatThrownBy(
            () ->
                ControlJwtDecoder.create(
                        (RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
                    .decode(token))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void validatesAnAsymmetricallySignedTokenAndPreservesItsSubject() throws Exception {
    String token =
        token("fraud-local", "fraud-control", "rule-author", Instant.now().plusSeconds(300));

    Jwt jwt =
        ControlJwtDecoder.create((RSAPublicKey) keys.getPublic(), "fraud-local", "fraud-control")
            .decode(token);

    assertThat(jwt.getSubject()).isEqualTo("rule-author");
  }

  private String token(String issuer, String audience, String subject, Instant expires)
      throws Exception {
    SignedJWT jwt =
        new SignedJWT(
            new JWSHeader(JWSAlgorithm.RS256),
            new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(audience)
                .subject(subject)
                .issueTime(Date.from(Instant.now().minusSeconds(600)))
                .expirationTime(expires == null ? null : Date.from(expires))
                .build());
    jwt.sign(new RSASSASigner((RSAPrivateKey) keys.getPrivate()));
    return jwt.serialize();
  }

  private static KeyPair generateKeys() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      return generator.generateKeyPair();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
