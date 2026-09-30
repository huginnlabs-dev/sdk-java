package dev.huginnlabs.dataflow;

import java.util.concurrent.Callable;

/**
 * Crash capture: turns unhandled {@link Throwable}s into wire errors. The
 * server renders the crash; the SDK only records it — on the current span
 * when one is active, else on a synthetic span — with the wire shape:
 * status {@code 500}, {@code error_message} = the exception's
 * {@code toString()} clipped to 500 chars, metadata {@code error.stack} =
 * the stack trace capped at 8192 chars from the top (deepest frames
 * survive the cut).
 *
 * <p>Nothing is swallowed: {@link Dataflow#capture(Runnable)} and
 * {@link Dataflow#captureCallable(Callable)} record, then rethrow (checked
 * throwables the {@link Runnable} contract cannot declare are wrapped in
 * {@link RuntimeException}; a {@link Callable}'s checked exceptions
 * propagate as declared). The uncaught-exception hook installed by
 * {@link Dataflow#captureUncaught()} cannot rethrow by definition — after
 * recording it chains to the previously installed handler, or reproduces
 * the JVM's standard stderr report when none was set.
 *
 * <p>Everything is best-effort: recording never throws into the caller,
 * and with the SDK disabled all entry points pass straight through.
 */
final class Crash {

    /** Wire cap on the error_message (exception toString). */
    static final int MESSAGE_CAP = 500;
    /** Wire cap on the error.stack metadata value. */
    static final int STACK_CAP = 8192;
    /** Metadata key carrying the stack trace. */
    static final String STACK_ATTR = "error.stack";

    private Crash() {}

    // -- wrapper bodies (behind Dataflow.capture / captureCallable) ---------

    /** Runs {@code body}, records any crash, rethrows (checked wrapped). */
    static void run(Runnable body) {
        try {
            body.run();
        } catch (RuntimeException | Error e) {
            record(e, "exception");
            throw e;
        } catch (Throwable t) {
            // Checked throwable smuggled through the Runnable contract:
            // record it, then wrap so it can keep propagating.
            record(t, "exception");
            throw new RuntimeException(t);
        }
    }

    /** Calls {@code body}, records any crash, rethrows unchanged. */
    static <T> T call(Callable<T> body) throws Exception {
        try {
            return body.call();
        } catch (Exception | Error e) {
            record(e, "exception");
            throw e;
        }
    }

    // -- recording ----------------------------------------------------------

    /**
     * Records the crash on the current span; when none is active, on a
     * synthetic span named {@code syntheticName}, ended immediately so the
     * event ships. Never throws.
     */
    static void record(Throwable t, String syntheticName) {
        if (t == null || !Dataflow.enabled()) return;
        try {
            Span span = Span.current();
            if (span != null) {
                mark(span, t);
                return;
            }
            Span synthetic = new Span(syntheticName, "FUNCTION_CALL", null);
            mark(synthetic, t);
            synthetic.end();
        } catch (RuntimeException ignore) {
            // best-effort bookkeeping — recording must never break the caller
        }
    }

    private static void mark(Span span, Throwable t) {
        span.status(500);
        span.recordError(clip(t.toString(), MESSAGE_CAP));
        span.attr(STACK_ATTR, stackTrace(t));
    }

    /**
     * Standard Java frame list — {@code "at Class.method(File:line)"} under
     * the exception header line — capped at {@link #STACK_CAP} chars from
     * the top.
     */
    static String stackTrace(Throwable t) {
        StringBuilder sb = new StringBuilder(t.toString()).append('\n');
        for (StackTraceElement f : t.getStackTrace()) {
            sb.append("at ").append(f.getClassName()).append('.').append(f.getMethodName())
                    .append('(').append(location(f)).append(")\n");
            if (sb.length() >= STACK_CAP) break;
        }
        return clip(sb.toString(), STACK_CAP);
    }

    /** Mirrors the JVM's frame suffix: "(Native Method)" / "(Unknown Source)". */
    private static String location(StackTraceElement f) {
        if (f.isNativeMethod()) return "Native Method";
        if (f.getFileName() == null) return "Unknown Source";
        return f.getFileName() + (f.getLineNumber() >= 0 ? ":" + f.getLineNumber() : "");
    }

    /** Hard cap from the top; the tail is dropped, never the head. */
    static String clip(String s, int cap) {
        if (s == null) return "";
        return s.length() > cap ? s.substring(0, cap) : s;
    }

    // -- uncaught-exception hook ---------------------------------------------

    /**
     * Installs the crash-recording default uncaught-exception handler,
     * wrapping whatever handler (or none) was installed before. Idempotent:
     * a second call is a no-op while the hook is ours. No-op while the SDK
     * is disabled.
     */
    static synchronized void installUncaught() {
        if (!Dataflow.enabled()) return;
        Thread.UncaughtExceptionHandler current = Thread.getDefaultUncaughtExceptionHandler();
        if (current instanceof CrashHandler) return; // already installed
        Thread.setDefaultUncaughtExceptionHandler(new CrashHandler(current));
    }

    /** Removes the crash hook, restoring the handler wrapped at install time. */
    static synchronized void ignoreUncaught() {
        Thread.UncaughtExceptionHandler h = Thread.getDefaultUncaughtExceptionHandler();
        if (h instanceof CrashHandler) {
            Thread.setDefaultUncaughtExceptionHandler(((CrashHandler) h).previous);
        }
    }

    private static final class CrashHandler implements Thread.UncaughtExceptionHandler {
        /** Handler (possibly null) installed before ours took over. */
        final Thread.UncaughtExceptionHandler previous;

        CrashHandler(Thread.UncaughtExceptionHandler previous) {
            this.previous = previous;
        }

        @Override public void uncaughtException(Thread thread, Throwable e) {
            record(e, "uncaught exception");
            Thread.UncaughtExceptionHandler prev = previous;
            if (prev != null) {
                prev.uncaughtException(thread, e);
                return;
            }
            // No previous handler: the JVM would have printed the default
            // report and installing ours silences it — reproduce it.
            System.err.print("Exception in thread \"" + thread.getName() + "\" ");
            e.printStackTrace(System.err);
        }
    }
}
