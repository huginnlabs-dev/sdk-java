package dev.huginnlabs.dataflow;

/**
 * AutoCloseable scope that makes a span the thread's current one and ends
 * it on close — the Java idiom replacing C++/Go RAII:
 *
 * <pre>{@code
 * try (Trace t = Dataflow.trace("booking.Confirm")) {
 *     t.span().setData("booking", "bk_42");
 *     ... // nested Dataflow.trace(...) calls join the same trace
 * }
 * }</pre>
 */
public final class Trace implements AutoCloseable {

    private final Span span;
    private final Span previous;

    public Trace(String name) { this(name, "FUNCTION_CALL"); }

    public Trace(String name, String type) {
        this.previous = Span.current();
        this.span = Dataflow.startSpan(name, type);
        this.span.activate();
    }

    /** The active span; attach payload fields, errors and status to it. */
    public Span span() { return span; }

    @Override public void close() {
        span.end();
        if (previous != null) previous.activate();
        else span.deactivate();
    }
}
