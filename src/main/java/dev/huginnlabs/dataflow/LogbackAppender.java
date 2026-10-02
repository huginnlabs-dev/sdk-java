package dev.huginnlabs.dataflow;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;

/**
 * Logback appender: forwards every {@link ILoggingEvent} into the log
 * pipeline behind {@code Dataflow.info/warn/error/debug} — the same bounded
 * queue, trace/span stamping and batched shipping the JUL bridge
 * ({@code Dataflow.LogHandler}) feeds. SLF4J/Logback is the default logging
 * stack of Spring Boot and most Java services, so for them this is the
 * zero-code bridge:
 *
 * <pre>{@code
 * Dataflow.installLogback();   // root logger of the default LoggerContext
 * }</pre>
 *
 * <p>Level mapping: {@code TRACE}/{@code DEBUG}→{@code debug},
 * {@code INFO}→{@code info}, {@code WARN}→{@code warn},
 * {@code ERROR}→{@code error}. The formatted message ({@code {}}
 * parameters resolved) ships; key-value pairs and markers are skipped in
 * v1. When the formatted message is empty but the event carries a
 * throwable, the throwable's {@code "ClassName: message"} stands in as the
 * message — an event with a crash is never shipped empty.
 *
 * <p>Everything is best-effort: {@code append} never throws into the
 * logging framework and drops silently while the SDK is disabled (or while
 * logging is off — no resolvable HTTP base). Lifecycle is the standard
 * {@link AppenderBase} one: {@link #start()} before events flow,
 * {@link #stop()} on detach — {@code Dataflow.installLogback()} and
 * {@code Dataflow.uninstallLogback()} do both.
 */
public class LogbackAppender extends AppenderBase<ILoggingEvent> {

    /** Name the convenience installs register the appender under. */
    public static final String NAME = "dataflow";

    private static final Object INSTALL_LOCK = new Object();
    /** Appender installed by {@link #install}/{@link #installDefault}. */
    private static volatile LogbackAppender installed;
    /** Context it was installed on (uninstall detaches from there). */
    private static volatile LoggerContext installedContext;

    public LogbackAppender() {
        setName(NAME);
    }

    // -- appender -------------------------------------------------------------

    /**
     * Forwards the event into the log pipeline. Never throws and records
     * nothing while the SDK is disabled — {@link Logs#record} drops
     * silently in both cases.
     */
    @Override protected void append(ILoggingEvent event) {
        if (event == null) return;
        try {
            Logs.record(levelOf(event.getLevel()), messageOf(event), null);
        } catch (Throwable ignore) {
            // logging must never break the logging framework
        }
    }

    /**
     * Maps Logback levels onto the wire's {@code debug|info|warn|error};
     * unknown levels report as {@code info} — mirroring
     * {@link Logs#normalize(String)}.
     */
    static String levelOf(Level level) {
        if (level == null) return "info";
        switch (level.toInt()) {
            case Level.TRACE_INT:
            case Level.DEBUG_INT:
                return "debug";
            case Level.WARN_INT:
                return "warn";
            case Level.ERROR_INT:
                return "error";
            default:
                return "info";
        }
    }

    /**
     * The event's formatted message ({@code {}} parameters resolved); when
     * that is empty, a carried throwable stands in as
     * {@code "ClassName: message"}.
     */
    static String messageOf(ILoggingEvent event) {
        String message = event.getFormattedMessage();
        if (message != null && !message.isEmpty()) return message;
        IThrowableProxy thrown = event.getThrowableProxy();
        if (thrown == null) return message == null ? "" : message;
        String detail = thrown.getMessage();
        return thrown.getClassName() + (detail == null || detail.isEmpty() ? "" : ": " + detail);
    }

    // -- install / uninstall (behind Dataflow.installLogback/uninstallLogback)

    /**
     * Installs the appender on the default SLF4J factory — the Logback
     * {@link LoggerContext} behind {@code LoggerFactory} — replacing
     * (never doubling) a previous install. Skips silently when SLF4J is
     * bound to a non-Logback backend.
     */
    public static void installDefault() {
        if (LoggerFactory.getILoggerFactory() instanceof LoggerContext ctx) {
            install(ctx);
        }
    }

    /**
     * Installs the appender on {@code ctx}'s root logger. Idempotent: an
     * appender installed earlier (on any context) is detached first, and
     * stale {@value #NAME}-named copies on this root are removed too — a
     * repeated install never double-ships.
     */
    public static void install(LoggerContext ctx) {
        if (ctx == null) throw new NullPointerException("ctx");
        synchronized (INSTALL_LOCK) {
            uninstall();
            ch.qos.logback.classic.Logger root =
                    ctx.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
            while (root.detachAppender(NAME)) {
                // drop stale/manual copies as well — exactly one of ours may ship
            }
            LogbackAppender appender = new LogbackAppender();
            appender.setContext(ctx);
            appender.start();
            root.addAppender(appender);
            installed = appender;
            installedContext = ctx;
        }
    }

    /**
     * Detaches and stops the appender installed by
     * {@link #install}/{@link #installDefault}. No-op (never throws) when
     * nothing is installed.
     */
    public static void uninstall() {
        synchronized (INSTALL_LOCK) {
            LogbackAppender appender = installed;
            LoggerContext ctx = installedContext;
            installed = null;
            installedContext = null;
            if (appender != null && ctx != null) {
                ctx.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME)
                        .detachAppender(appender);
                appender.stop();
            }
        }
    }
}
