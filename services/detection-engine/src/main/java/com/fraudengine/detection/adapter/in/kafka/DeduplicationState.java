package com.fraudengine.detection.adapter.in.kafka;

record DeduplicationState(String fingerprint, long seenAtStreamTimeMillis) {}
