package dev.huginnlabs.dataflow;

import java.util.Map;

/**
 * Minimal JSON writer for payload snapshots — no third-party dependencies.
 * Handles maps, iterables, strings, numbers, booleans and null; unknown
 * objects fall back to toString(). {@link Raw} embeds a pre-serialized
 * JSON document.
 */
public final class Json {

    /** Wrapper for an already-serialized JSON value. */
    public static final class Raw {
        final String json;
        public Raw(String json) { this.json = json; }
    }

    private Json() {}

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder(128);
        write(sb, value);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object v) {
        if (v == null) { sb.append("null"); return; }
        if (v instanceof Raw r) { sb.append(r.json); return; }
        if (v instanceof String s) { escape(sb, s); return; }
        if (v instanceof Number || v instanceof Boolean) { sb.append(v); return; }
        if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                escape(sb, String.valueOf(e.getKey()));
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
            return;
        }
        if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(',');
                first = false;
                write(sb, o);
            }
            sb.append(']');
            return;
        }
        escape(sb, String.valueOf(v));
    }

    static void escape(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append('"');
    }
}
