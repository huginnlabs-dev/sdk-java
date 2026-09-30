package dev.huginnlabs.dataflow;

import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Statement summarizing vectors — same table as the Kotlin sibling's
 * TransportTest (and the Go SDK's TestStmtSummary).
 */
@TestClassOrder(ClassOrderer.ClassName.class)
class StmtSummaryTest {

    @Test
    void stmtSummaryDerivesVerbAndTable() {
        String[][] cases = {
                // Same table as the Go SDK's TestStmtSummary.
                {"SELECT id, total FROM orders WHERE id = $1", "SELECT orders"},
                {"  insert into users (email) values ($1)", "INSERT users"},
                {"UPDATE public.items SET total = total - 1", "UPDATE items"},
                {"DELETE FROM sessions WHERE expires < now()", "DELETE sessions"},
                {"CREATE TABLE IF NOT EXISTS migrations (id int)", "CREATE migrations"},
                {"select u.id\nfrom users u\njoin orders o on o.user_id = u.id", "SELECT users"},
                {"PRAGMA journal_mode=WAL", "PRAGMA"},
                // Extras.
                {"CREATE TABLE \"logs\" (id int)", "CREATE logs"},
                {"INSERT INTO public.orders (id) VALUES (1)", "INSERT orders"},
                {"EXPLAIN ANALYZE SELECT * FROM t", "EXPLAIN t"},
                {"WITH recent AS (SELECT 1 FROM orders) SELECT * FROM recent", "WITH orders"},
                {"BEGIN", "BEGIN"},
                {"SET statement_timeout = 5000", "SET"},
                {"MERGE INTO inventory USING cart ON ...", "MERGE"},
                {"", "QUERY"},
                {"   ", "QUERY"},
                {null, "QUERY"},
        };
        for (String[] c : cases) {
            assertEquals(c[1], Jdbc.stmtSummary(c[0]), "stmtSummary(" + c[0] + ")");
        }
    }

    @Test
    void clipStatementCollapsesWhitespaceAndCapsAt200() {
        assertEquals("SELECT a b c", Jdbc.clipStatement("SELECT a\n   b\t\tc"));
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < 100; i++) sb.append("x, ");
        sb.append('1');
        String clipped = Jdbc.clipStatement(sb.toString());
        assertEquals(200, clipped.length());
        assertTrue(clipped.startsWith("SELECT "));
        assertEquals("short query", Jdbc.clipStatement("short query"));
        assertEquals("", Jdbc.clipStatement(null));
        assertEquals("", Jdbc.clipStatement("   "));
    }
}
