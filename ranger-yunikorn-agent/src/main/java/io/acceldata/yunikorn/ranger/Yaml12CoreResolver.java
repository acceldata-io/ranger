/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.yunikorn.ranger;

import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.resolver.Resolver;

import java.util.regex.Pattern;

/**
 * A SnakeYAML {@link Resolver} that resolves scalar types using YAML 1.2
 * core-schema semantics instead of SnakeYAML's default YAML 1.1 rules.
 *
 * <h2>Why this exists</h2>
 * SnakeYAML's default resolver follows YAML 1.1, which implicitly types a set
 * of scalars that look innocuous in a YuniKorn {@code queues.yaml} but are NOT
 * what the operator meant. Because {@link AclSplicer} round-trips the whole
 * document (load → mutate ACLs → dump), every scalar is re-resolved on dump,
 * so these get silently rewritten:
 *
 * <pre>
 *   yes / no / on / off   →  true / false      ("the Norway problem")
 *   0755                  →  493               (parsed as octal)
 *   3:30                  →  210               (parsed as base-60 / sexagesimal)
 * </pre>
 *
 * That breaks the splicer's core invariant — "we only touch submitacl/adminacl;
 * every other field passes through unchanged" — for operator-owned values such
 * as queue {@code properties}, labels, and placement-rule strings.
 *
 * <h2>What this changes</h2>
 * Only the implicit <em>resolvers</em> are narrowed:
 * <ul>
 *   <li><b>bool</b> — only {@code true}/{@code false} (any case). {@code yes},
 *       {@code no}, {@code on}, {@code off}, {@code y}, {@code n} stay strings.</li>
 *   <li><b>int</b> — plain decimals with no leading zero ({@code 0}, {@code 42},
 *       {@code -7}) plus explicit {@code 0o…} octal and {@code 0x…} hex.
 *       Leading-zero forms like {@code 0755} and base-60 forms like {@code 3:30}
 *       stay strings.</li>
 *   <li><b>float</b> — requires a decimal point or exponent ({@code 1.5},
 *       {@code 1e3}, {@code .inf}, {@code .nan}); plain integers are <em>not</em>
 *       swept up as floats.</li>
 *   <li><b>null</b> / <b>merge</b> ({@code <<}) — unchanged from the default.</li>
 *   <li><b>timestamp</b> / the {@code =} value key — deliberately NOT resolved
 *       (no YAML 1.1 implicit dates), so date-like strings stay strings.</li>
 * </ul>
 *
 * <p>Genuine numeric fields YuniKorn expects (weights, {@code maxapplications},
 * vcores) still resolve to numbers, so the dumped document keeps them unquoted.
 * A value that IS meant to be the string {@code "50"} (i.e. originally quoted)
 * is re-quoted on dump, because this same resolver tells the emitter that a
 * bare {@code 50} would round-trip back to an int.
 */
final class Yaml12CoreResolver extends Resolver {

    // bool: true/false only (no yes/no/on/off).
    private static final Pattern BOOL_1_2 =
            Pattern.compile("^(?:true|True|TRUE|false|False|FALSE)$");

    // int: JSON-style decimal (no leading zeros), or explicit 0o octal / 0x hex.
    // Rejecting leading zeros is what keeps "0755" a string.
    private static final Pattern INT_1_2 =
            Pattern.compile("^(?:[-+]?(?:0|[1-9][0-9]*)|0o[0-7]+|0x[0-9a-fA-F]+)$");

    // float: must contain a '.' or an exponent, so plain integers don't match.
    private static final Pattern FLOAT_1_2 = Pattern.compile(
            "^[-+]?(?:\\.[0-9]+|[0-9]+\\.[0-9]*|[0-9]+(?:\\.[0-9]*)?[eE][-+]?[0-9]+)$"
            + "|^[-+]?\\.(?:inf|Inf|INF)$"
            + "|^\\.(?:nan|NaN|NAN)$");

    private static final Pattern NULL_1_2 =
            Pattern.compile("^(?:~|null|Null|NULL|)$");

    private static final Pattern EMPTY =
            Pattern.compile("^$");

    private static final Pattern MERGE =
            Pattern.compile("^(?:<<)$");

    /**
     * Register only the narrowed set. We do NOT call {@code super} so the
     * default YAML 1.1 resolvers (yes/no bool, octal/sexagesimal int,
     * timestamp, value) are never installed.
     */
    @Override
    protected void addImplicitResolvers() {
        addImplicitResolver(Tag.BOOL,  BOOL_1_2,  "tTfF");
        addImplicitResolver(Tag.INT,   INT_1_2,   "-+0123456789");
        addImplicitResolver(Tag.FLOAT, FLOAT_1_2, "-+0123456789.");
        addImplicitResolver(Tag.MERGE, MERGE,     "<");
        addImplicitResolver(Tag.NULL,  NULL_1_2,  "~nN\0");
        addImplicitResolver(Tag.NULL,  EMPTY,     null);
    }
}
