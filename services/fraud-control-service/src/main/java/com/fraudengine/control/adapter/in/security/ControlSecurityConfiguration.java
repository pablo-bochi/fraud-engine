package com.fraudengine.control.adapter.in.security;

import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class ControlSecurityConfiguration {
  @Bean
  @ConditionalOnMissingBean(RSAPublicKey.class)
  RSAPublicKey publicKey(
      @Value("${control.security.public-key-location}") Resource publicKeyResource)
      throws Exception {
    String pem = new String(publicKeyResource.getInputStream().readAllBytes());
    byte[] encoded =
        Base64.getMimeDecoder()
            .decode(
                pem.replace("-----BEGIN PUBLIC KEY-----", "")
                    .replace("-----END PUBLIC KEY-----", ""));
    return (RSAPublicKey)
        KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(encoded));
  }

  @Bean
  JwtDecoder jwtDecoder(
      RSAPublicKey publicKey,
      @Value("${control.security.issuer}") String issuer,
      @Value("${control.security.audience}") String audience) {
    return ControlJwtDecoder.create(publicKey, issuer, audience);
  }

  @Bean
  SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
    return http.csrf(csrf -> csrf.disable())
        .sessionManagement(
            session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers("/actuator/health", "/actuator/prometheus")
                    .permitAll()
                    .requestMatchers(
                        org.springframework.http.HttpMethod.GET,
                        "/api/v1/rules/**",
                        "/api/v1/rulesets/**")
                    .hasAuthority("RULE_READ")
                    .requestMatchers(
                        org.springframework.http.HttpMethod.GET, "/api/v1/audit-events/**")
                    .hasAuthority("AUDIT_READ")
                    .requestMatchers(
                        org.springframework.http.HttpMethod.POST,
                        "/api/v1/rules/*/versions/*/approval",
                        "/api/v1/rules/*/versions/*/rejection")
                    .hasAuthority("RULE_APPROVE")
                    .requestMatchers(org.springframework.http.HttpMethod.POST, "/api/v1/rules/**")
                    .hasAuthority("RULE_WRITE")
                    .anyRequest()
                    .authenticated())
        .oauth2ResourceServer(
            resource ->
                resource.jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter())))
        .build();
  }

  private JwtAuthenticationConverter jwtAuthenticationConverter() {
    JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(
        jwt -> {
          List<String> permissions = jwt.getClaimAsStringList("permissions");
          return (permissions == null ? List.<String>of() : permissions)
              .stream()
                  .map(SimpleGrantedAuthority::new)
                  .map(authority -> (org.springframework.security.core.GrantedAuthority) authority)
                  .toList();
        });
    return converter;
  }
}
