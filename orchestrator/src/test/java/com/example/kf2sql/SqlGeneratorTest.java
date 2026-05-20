package com.example.kf2sql;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 07.2 RED test for {@link SqlGenerator}.
 *
 * RED phase: this test is written BEFORE {@link SqlGenerator} exists. It must fail to compile
 * (or fail at runtime if you stub the class). After implementing the generator (07.3 GREEN),
 * the test passes.
 *
 * Equivalence is functional, not byte-for-byte: comments and blank lines are stripped before
 * comparison. The functional SQL body must match {@code 01-filter-F1.sql} exactly when the
 * generator is given the F1 subscription as input.
 */
class SqlGeneratorTest {

    private static final Path PROJECT_ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final Path TEMPLATE = PROJECT_ROOT.resolve("sql/01-filter-template.sql");
    private static final Path F1_SUBSCRIPTION = PROJECT_ROOT.resolve("test-data/fixtures/F1-subscription.json");
    private static final Path F1_EXPECTED_SQL = PROJECT_ROOT.resolve("sql/01-filter-F1.sql");

    @Test
    void fromSubscription_F1_matchesFixture() throws IOException {
        // Given: F1 subscription, F1 input/output topic names
        Subscription sub = Subscription.fromJson(Files.readString(F1_SUBSCRIPTION));
        String inputTopic = "kf-input-test";
        String outputTopic = "kf-data-test";

        // When: SqlGenerator produces the concrete SQL
        SqlGenerator gen = new SqlGenerator(Files.readString(TEMPLATE));
        String actual = gen.fromSubscription(sub, inputTopic, outputTopic);

        // Then: functional equality with 01-filter-F1.sql
        String expected = Files.readString(F1_EXPECTED_SQL);
        assertEquals(strip(expected), strip(actual),
            "Generator output must match 01-filter-F1.sql functionally (comments/blank-lines ignored)");
    }

    @Test
    void fromSubscription_emptyDataIdList_throws() throws IOException {
        SqlGenerator gen = new SqlGenerator(Files.readString(TEMPLATE));
        Subscription emptySub = new Subscription("V1", "c1", List.of());

        assertThrows(IllegalArgumentException.class,
            () -> gen.fromSubscription(emptySub, "in", "out"),
            "Empty dataIdList = unsubscribe; orchestrator should not call SqlGenerator for it.");
    }

    @Test
    void fromSubscription_singleMdc_producesSingletonInClause() throws IOException {
        SqlGenerator gen = new SqlGenerator(Files.readString(TEMPLATE));
        Subscription sub = new Subscription("V1", "c1", List.of("42"));
        String sql = gen.fromSubscription(sub, "kf-input-test", "kf-data-test");
        assertTrue(sql.contains("IN (42)"),
            "Single mdc list should produce IN (42), not IN (42,)");
    }

    /**
     * Strip SQL comments (lines starting with --) and blank lines. Normalize whitespace
     * (collapse multiple spaces). This makes the test robust to comment-header drift.
     */
    private static String strip(String sql) {
        StringBuilder out = new StringBuilder();
        for (String line : sql.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                continue;
            }
            out.append(trimmed.replaceAll("\\s+", " ")).append("\n");
        }
        return out.toString();
    }
}
