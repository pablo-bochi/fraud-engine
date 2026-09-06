package com.fraudengine.control.application;

import java.util.UUID;

public record RulesetStatus(
    UUID desiredSnapshotId, long desiredVersion, long publishedVersion, long pendingOutboxEvents) {}
