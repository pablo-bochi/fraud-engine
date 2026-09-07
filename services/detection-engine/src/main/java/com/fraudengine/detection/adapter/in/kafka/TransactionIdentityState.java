package com.fraudengine.detection.adapter.in.kafka;

record TransactionIdentityState(String eventId, long seenAtStreamTimeMillis) {}
