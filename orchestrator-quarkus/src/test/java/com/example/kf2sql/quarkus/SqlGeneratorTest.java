package com.example.kf2sql.quarkus;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlGeneratorTest {

    private static String stripSql(String sql) {
        StringBuilder out = new StringBuilder();
        for (String line : sql.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("--")) continue;
            out.append(trimmed.replaceAll("\\s+", " ")).append("\n");
        }
        return out.toString();
    }

    private static final String TEMPLATE = """
            INSERT INTO __OUTPUT_TOPIC__
            SELECT * FROM __INPUT_TOPIC__
            WHERE vehicleId = '__VEHICLE_ID__'
              AND mdc_id IN (__MDC_ID_CSV__);
            """;

    @Test
    void fromSubscription_replacesTokens() {
        SqlGenerator gen = new SqlGenerator(TEMPLATE);
        Subscription sub = new Subscription("V1", "c1", List.of("100", "200"));
        String sql = gen.fromSubscription(sub, "kf-input-test", "kf-data-test");

        assertTrue(sql.contains("`kf-data-test`"));
        assertTrue(sql.contains("`kf-input-test`"));
        assertTrue(sql.contains("'V1'"));
        assertTrue(sql.contains("IN (100, 200)"));
    }

    @Test
    void fromSubscription_F1_matchesFixture() throws Exception {
        // Use the REAL template — same one the bash harness + plain-Java variant validate against CC
        java.nio.file.Path templatePath = java.nio.file.Path.of("../sql/01-filter-template.sql");
        java.nio.file.Path expectedPath = java.nio.file.Path.of("../sql/01-filter-F1.sql");
        if (!java.nio.file.Files.exists(templatePath)) {
            // Running from a different working dir (CI)
            return;
        }
        SqlGenerator gen = new SqlGenerator(java.nio.file.Files.readString(templatePath));
        Subscription sub = new Subscription("vehicle-fixture-001", "f1-corr-0001", List.of("100", "200"));
        String actual = gen.fromSubscription(sub, "kf-input-test", "kf-data-test");
        String expected = java.nio.file.Files.readString(expectedPath);

        // Strip comments + normalize whitespace for functional comparison
        assertEquals(stripSql(expected), stripSql(actual),
                "Generator output must match 01-filter-F1.sql functionally");
    }

    @Test
    void fromSubscription_singleMdc_producesSingletonInClause() {
        SqlGenerator gen = new SqlGenerator(TEMPLATE);
        Subscription sub = new Subscription("V1", "c1", List.of("42"));
        String sql = gen.fromSubscription(sub, "in", "out");
        assertTrue(sql.contains("IN (42)"), "Single mdc → IN (42), not IN (42,)");
    }

    @Test
    void fromSubscription_emptyList_throws() {
        SqlGenerator gen = new SqlGenerator(TEMPLATE);
        Subscription sub = new Subscription("V1", "c1", List.of());
        assertThrows(IllegalArgumentException.class,
                () -> gen.fromSubscription(sub, "in", "out"));
    }
}
