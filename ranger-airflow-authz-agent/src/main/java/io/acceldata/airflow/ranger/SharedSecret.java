/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.airflow.ranger;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Shared secret authenticating the Airflow api-server to this agent.
 *
 * <p>Compared in constant time. Never logged. The file must be mode {@code 0400}
 * (no group/other bits) and hold at least 32 bytes of token material.
 */
public final class SharedSecret {

    private static final int MIN_TOKEN_BYTES = 32;
    private static final String BEARER_PREFIX = "bearer ";

    private final byte[] token;

    SharedSecret(byte[] token) {
        this.token = Objects.requireNonNull(token, "token");
        if (token.length < MIN_TOKEN_BYTES) {
            throw new IllegalArgumentException(
                    "shared secret is " + token.length + " bytes; need at least " + MIN_TOKEN_BYTES);
        }
    }

    public static SharedSecret fromFile(Path path) {
        Objects.requireNonNull(path, "path");
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("token file missing or not a file: " + path);
        }
        try {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
            if (perms.contains(PosixFilePermission.GROUP_READ)
                    || perms.contains(PosixFilePermission.GROUP_WRITE)
                    || perms.contains(PosixFilePermission.GROUP_EXECUTE)
                    || perms.contains(PosixFilePermission.OTHERS_READ)
                    || perms.contains(PosixFilePermission.OTHERS_WRITE)
                    || perms.contains(PosixFilePermission.OTHERS_EXECUTE)) {
                throw new IllegalArgumentException(
                        "token file " + path + " must be mode 0400 (no group/other bits)");
            }
            String raw = Files.readString(path).strip();
            if (raw.isEmpty()) {
                throw new IllegalArgumentException("token file is empty: " + path);
            }
            return new SharedSecret(raw.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to read token file " + path + ": " + e.getMessage(), e);
        }
    }

    /**
     * @param authorizationHeader the raw {@code Authorization} header, or null
     * @return true only when the header is {@code Bearer <token>} and the token matches
     */
    public boolean matches(String authorizationHeader) {
        if (authorizationHeader == null) {
            dummyCompare();
            return false;
        }
        String trimmed = authorizationHeader.strip();
        if (trimmed.length() < BEARER_PREFIX.length()
                || !trimmed.substring(0, BEARER_PREFIX.length()).toLowerCase(Locale.ROOT).equals(BEARER_PREFIX)) {
            dummyCompare();
            return false;
        }
        byte[] presented = trimmed.substring(BEARER_PREFIX.length()).strip().getBytes(StandardCharsets.UTF_8);
        if (presented.length != token.length) {
            dummyCompare();
            return false;
        }
        return MessageDigest.isEqual(token, presented);
    }

    private void dummyCompare() {
        MessageDigest.isEqual(token, token);
    }
}
