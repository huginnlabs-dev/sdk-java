package dev.huginnlabs.dataflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import dev.huginnlabs.dataflow.gen.DataflowProto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash capture: {@code Dataflow.capture} / {@code captureCallable} /
 * {@code captureUncaught}, asserted through {@link SdkTestEnv}'s capture
 * sink. The class name sorts after DataflowDisabledTest on purpose — that
 * suite proves the disabled passthrough contract before any configure()
 * call; this one needs the SDK enabled.
 */
@TestClassOrder(ClassOrderer.ClassName.class)
class SdkCrashTest {

    @BeforeAll
    static void enableSdk() {
        SdkTestEnv.configure();
    }

    /** Test-only: smuggles a checked throwable through a Runnable lambda. */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> E sneaky(Throwable t) throws E {
        throw (E) t;
    }

    @Test
    void captureRecordsCurrentSpanAndRethrows() {
        AtomicReference<RuntimeException> thrown = new AtomicReference<>();
        // The crash lands on the CURRENT span, which ships at close() — so
        // close() must happen inside the capture window.
        Trace parent = Dataflow.trace("job.Run");
        SdkTestEnv.startCapture();
        try {
            Dataflow.capture(() -> { throw new IllegalStateException("boom"); });
        } catch (RuntimeException e) {
            thrown.set(e);
        } finally {
            parent.close();
            SdkTestEnv.stopCapture();
        }
        assertTrue(thrown.get() instanceof IllegalStateException);
        assertEquals(1, SdkTestEnv.events().size());
        DataflowProto.TraceEvent ev = SdkTestEnv.events().get(0);
        assertEquals("job.Run", ev.getName());
        assertEquals(500, ev.getStatusCode());
        assertEquals("java.lang.IllegalStateException: boom", ev.getErrorMessage());
        String stack = ev.getMetadataOrThrow("error.stack");
        assertTrue(stack.startsWith("java.lang.IllegalStateException: boom\n"));
        assertTrue(stack.contains("at dev.huginnlabs.dataflow.SdkCrashTest."));
    }

    @Test
    void captureOpensSyntheticExceptionSpanWithoutCurrent() {
        SdkTestEnv.startCapture();
        try {
            assertThrows(ArithmeticException.class,
                    () -> Dataflow.capture(() -> { throw new ArithmeticException("/0"); }));
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertEquals(1, SdkTestEnv.events().size());
        DataflowProto.TraceEvent ev = SdkTestEnv.events().get(0);
        assertEquals("exception", ev.getName());
        assertEquals(500, ev.getStatusCode());
        assertEquals("java.lang.ArithmeticException: /0", ev.getErrorMessage());
        assertTrue(ev.getMetadataOrThrow("error.stack").contains("at "));
    }

    @Test
    void captureCallableReturnsValueAndRecordsDeclaredThrowables() throws Exception {
        assertEquals("ok", Dataflow.captureCallable(() -> "ok"));

        SdkTestEnv.startCapture();
        try {
            assertThrows(java.io.IOException.class,
                    () -> Dataflow.captureCallable(() -> { throw new java.io.IOException("io"); }));
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertEquals(1, SdkTestEnv.events().size());
        DataflowProto.TraceEvent ev = SdkTestEnv.events().get(0);
        assertEquals("exception", ev.getName());
        assertEquals(500, ev.getStatusCode());
        assertEquals("java.io.IOException: io", ev.getErrorMessage());
    }

    @Test
    void captureWrapsCheckedThrowablesFromRunnable() {
        java.io.IOException checked = new java.io.IOException("io");
        SdkTestEnv.startCapture();
        RuntimeException wrapped;
        try {
            wrapped = assertThrows(RuntimeException.class, () -> Dataflow.capture(() -> sneaky(checked)));
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertSame(checked, wrapped.getCause());
        // The ORIGINAL throwable's shape is what got recorded.
        assertEquals(1, SdkTestEnv.events().size());
        assertEquals("java.io.IOException: io", SdkTestEnv.events().get(0).getErrorMessage());
        assertEquals(500, SdkTestEnv.events().get(0).getStatusCode());
    }

    @Test
    void stackTraceFormatAndCap() {
        Throwable t = new RuntimeException("boom");
        String s = Crash.stackTrace(t);
        assertTrue(s.startsWith("java.lang.RuntimeException: boom\n"));
        assertTrue(s.contains("at dev.huginnlabs.dataflow.SdkCrashTest.stackTraceFormatAndCap(SdkCrashTest.java:"));
        assertTrue(s.endsWith(")\n"));
        assertTrue(s.length() <= Crash.STACK_CAP);

        // Overflow: 2000 synthetic frames — capped at exactly STACK_CAP
        // chars from the top, tail dropped.
        StackTraceElement[] frames = new StackTraceElement[2000];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = new StackTraceElement("pkg.Service" + i, "method" + i, "Service.java", 10 + i);
        }
        t.setStackTrace(frames);
        String deep = Crash.stackTrace(t);
        assertEquals(Crash.STACK_CAP, deep.length());
        assertTrue(deep.contains("at pkg.Service0.method0(Service.java:10)"));
        assertFalse(deep.contains("Service1999"));
    }

    @Test
    void wireCapsMessageAndStack() {
        SdkTestEnv.startCapture();
        try {
            assertThrows(RuntimeException.class, () -> Dataflow.capture(() -> {
                throw new RuntimeException("x".repeat(2000));
            }));
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertEquals(1, SdkTestEnv.events().size());
        DataflowProto.TraceEvent ev = SdkTestEnv.events().get(0);
        assertEquals(Crash.MESSAGE_CAP, ev.getErrorMessage().length());
        assertTrue(ev.getMetadataOrThrow("error.stack").length() <= Crash.STACK_CAP);
    }

    @Test
    void uncaughtRecordsThenChainsToPreviousHandler() {
        Thread.UncaughtExceptionHandler original = Thread.getDefaultUncaughtExceptionHandler();
        List<String> order = new ArrayList<>();
        Thread.UncaughtExceptionHandler previous = (t, e) -> order.add("previous:" + e.getMessage());
        Thread.setDefaultUncaughtExceptionHandler(previous);
        try {
            Dataflow.captureUncaught();
            Dataflow.captureUncaught(); // idempotent: must not double-wrap

            Thread.UncaughtExceptionHandler hook = Thread.getDefaultUncaughtExceptionHandler();
            assertNotNull(hook);
            assertTrue(hook != previous);

            SdkTestEnv.startCapture();
            try {
                hook.uncaughtException(Thread.currentThread(), new RuntimeException("fatal"));
            } finally {
                SdkTestEnv.stopCapture();
            }

            assertEquals(List.of("previous:fatal"), order);

            assertEquals(1, SdkTestEnv.events().size());
            DataflowProto.TraceEvent ev = SdkTestEnv.events().get(0);
            assertEquals("uncaught exception", ev.getName());
            assertEquals(500, ev.getStatusCode());
            assertEquals("java.lang.RuntimeException: fatal", ev.getErrorMessage());
            assertTrue(ev.getMetadataOrThrow("error.stack").contains("at "));

            // Exactly one dataflow layer: a single ignoreUncaught() lands on
            // the handler we wrapped.
            Dataflow.ignoreUncaught();
            assertSame(previous, Thread.getDefaultUncaughtExceptionHandler());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(original);
        }
    }

    @Test
    void uncaughtWithNoPreviousHandlerKeepsDefaultReport() {
        Thread.UncaughtExceptionHandler original = Thread.getDefaultUncaughtExceptionHandler();
        PrintStream originalErr = System.err;
        Thread.setDefaultUncaughtExceptionHandler(null);
        try {
            Dataflow.captureUncaught();
            Thread.UncaughtExceptionHandler hook = Thread.getDefaultUncaughtExceptionHandler();
            assertNotNull(hook);

            ByteArrayOutputStream err = new ByteArrayOutputStream();
            System.setErr(new PrintStream(err, true));
            SdkTestEnv.startCapture();
            try {
                hook.uncaughtException(Thread.currentThread(), new RuntimeException("naked"));
            } finally {
                SdkTestEnv.stopCapture();
                System.setErr(originalErr);
            }
            // Recording still happened…
            assertEquals(1, SdkTestEnv.events().size());
            assertEquals("uncaught exception", SdkTestEnv.events().get(0).getName());
            // …and the JVM's standard report was reproduced.
            String report = err.toString();
            assertTrue(report.contains(
                    "Exception in thread \"" + Thread.currentThread().getName() + "\" "));
            assertTrue(report.contains("java.lang.RuntimeException: naked"));
            assertTrue(report.contains("at "));
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(original);
        }
    }

    @Test
    void ignoreUncaughtWithoutInstallIsNoOp() {
        Thread.UncaughtExceptionHandler before = Thread.getDefaultUncaughtExceptionHandler();
        Dataflow.ignoreUncaught(); // never installed here — must not throw or change anything
        assertSame(before, Thread.getDefaultUncaughtExceptionHandler());
    }
}
