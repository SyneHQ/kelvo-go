// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package com.synehq.kelvo.jdbc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

final class Protocol {
    static final int MAX_INPUT = 3 << 20;
    static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(32).maxStringLength(MAX_INPUT).maxNumberLength(1000).build()).build())
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);

    record Input(String id, String digest, JsonNode request, JsonNode source,
                 long rows, long bytes, int batchRows, long deadlineMillis, long credentialsMillis, long directBytes, List<String> stepDigests) {}

    static Input read(InputStream input) throws Exception {
        byte[] raw = input.readNBytes(MAX_INPUT + 1);
        try { return parse(raw, System.currentTimeMillis()); }
        finally { Arrays.fill(raw, (byte) 0); }
    }

    static Input parse(byte[] raw, long now) throws Exception {
        if (raw.length == 0 || raw.length > MAX_INPUT) throw invalid();
        int start = -1, end = -1;
        try (JsonParser parser = JSON.getFactory().createParser(raw)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) throw invalid();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) throw invalid();
                String field = parser.currentName();
                if (parser.nextToken() == null) throw invalid();
                if (field.equals("request")) {
                    if (parser.currentToken() != JsonToken.START_OBJECT) throw invalid();
                    start = Math.toIntExact(parser.currentTokenLocation().getByteOffset());
                    parser.skipChildren();
                    end = Math.toIntExact(parser.currentTokenLocation().getByteOffset()) + 1;
                } else parser.skipChildren();
            }
            if (parser.nextToken() != null) throw invalid();
        }
        if (start < 0 || end <= start || end - start > 256 << 10) throw invalid();
        JsonNode root = JSON.readTree(raw);
        keys(root, "version", "operation_id", "request_sha256", "request", "limits", "source", "app_team", "input", "credentials_valid_until", "expires_at", "runtime");
        if (integer(root, "version") != 1 || root.has("input") && !root.path("input").asText().isEmpty()) throw invalid();
        String id = text(root, "operation_id", 128), digest = text(root, "request_sha256", 64);
        if (!id.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}") || !digest.matches("[0-9a-f]{64}")) throw invalid();
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        hash.update("kelvo.database.operation.v1\0".getBytes(StandardCharsets.UTF_8));
        hash.update(raw, start, end - start);
        if (!MessageDigest.isEqual(HexFormat.of().parseHex(digest), hash.digest())) throw invalid();
        JsonNode request = root.path("request"), source = root.path("source"), limits = root.path("limits");
        keys(request, "version", "kind", "connection", "idempotency_key", "approval_id", "spec");
        keys(source, "engine", "dsn", "url", "username", "password", "token", "options", "tenant_id", "connection_id", "revision", "database", "schema");
        keys(limits, "max_rows", "max_bytes", "batch_rows", "timeout_ms", "memory_mb", "threads", "max_temp_mb");
        if (integer(request, "version") != 1) throw invalid();
        JsonNode connection = request.path("connection");
        keys(connection, "id", "database", "schema");
        if (!text(source, "connection_id", 256).equals(text(connection, "id", 128)) ||
            !optional(source, "database", 65536).equals(optional(connection, "database", 65536)) ||
            !optional(source, "schema", 65536).equals(optional(connection, "schema", 65536))) throw invalid();
        text(source, "tenant_id", 256); text(source, "revision", 256);
        long rows = integer(limits, "max_rows"), bytes = integer(limits, "max_bytes"), batch = integer(limits, "batch_rows"), timeout = integer(limits, "timeout_ms");
        long expiry = Math.multiplyExact(integer(root, "expires_at"), 1000), credentials = Math.multiplyExact(integer(root, "credentials_valid_until"), 1000);
        if (rows < 1 || rows > 1_000_000 || bytes < 1 || bytes > 64 << 20 || batch < 1 || batch > 4096 || timeout < 1 || timeout > 300_000 || expiry <= now || expiry > now + 300_000 || credentials <= now || credentials > now + 5000 || credentials > expiry) throw invalid();
        JsonNode runtime = root.path("runtime");
        keys(runtime, "version", "java_fd", "jar_fds", "heap_mb", "direct_mb");
        long heap = integer(runtime,"heap_mb"), direct = integer(runtime,"direct_mb");
        if (integer(runtime,"version") != 1 || integer(runtime,"java_fd") != 7 || heap < 32 || heap > 4096 || direct < 16 || direct > 4096 || !runtime.path("jar_fds").isArray() || runtime.path("jar_fds").size() < 1 || runtime.path("jar_fds").size() > 32) throw invalid();
        int descriptor = 8;
        for (JsonNode fd : runtime.path("jar_fds")) if (!fd.isIntegralNumber() || fd.intValue() != descriptor++) throw invalid();
        return new Input(id, digest, request, source, rows, bytes, (int) batch, Math.min(expiry, now + timeout), credentials, direct << 20, stepDigests(raw));
    }

    // Hash exact Go canonical array entries, never a Java-reserialized map.
    static List<String> stepDigests(byte[] raw) throws Exception {
        List<String> result = new ArrayList<>();
        try (JsonParser parser = JSON.getFactory().createParser(raw)) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() == JsonToken.START_OBJECT && parser.getParsingContext().pathAsPointer().toString().matches("/request/spec/statement/batch/statements/[0-9]+")) {
                    int start = Math.toIntExact(parser.currentTokenLocation().getByteOffset());
                    parser.skipChildren();
                    int end = Math.toIntExact(parser.currentTokenLocation().getByteOffset()) + 1;
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    digest.update("kelvo.operation.statement.v1\0".getBytes(StandardCharsets.UTF_8));
                    digest.update(raw, start, end-start);
                    result.add(HexFormat.of().formatHex(digest.digest()));
                }
            }
        }
        return List.copyOf(result);
    }

    static void keys(JsonNode node, String... allowed) {
        if (!node.isObject()) throw invalid();
        Set<String> names = Set.of(allowed);
        node.fieldNames().forEachRemaining(name -> { if (!names.contains(name)) throw invalid(); });
    }

    static long integer(JsonNode node, String name) {
        JsonNode value = node.path(name);
        if (!value.isIntegralNumber() || !value.canConvertToLong()) throw invalid();
        return value.longValue();
    }

    static String text(JsonNode node, String name, int max) {
        String value = optional(node, name, max);
        if (value.isEmpty()) throw invalid();
        return value;
    }

    static String optional(JsonNode node, String name, int max) {
        if (!node.has(name)) return "";
        JsonNode value = node.get(name);
        if (!value.isTextual() || value.textValue().length() > max || value.textValue().indexOf(0) >= 0) throw invalid();
        return value.textValue();
    }

    static IllegalArgumentException invalid() { return new IllegalArgumentException("INVALID_ARGUMENT"); }
    static UnsupportedOperationException unsupported() { return new UnsupportedOperationException("UNSUPPORTED"); }
}
