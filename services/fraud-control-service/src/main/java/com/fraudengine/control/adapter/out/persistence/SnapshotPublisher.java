package com.fraudengine.control.adapter.out.persistence;

@FunctionalInterface
public interface SnapshotPublisher {
  void publish(String key, String canonicalPayload);
}
