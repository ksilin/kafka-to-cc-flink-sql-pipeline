package com.example.kf2sql;

import java.io.IOException;
import java.util.List;

/**
 * Abstracts {@link ProcessBuilder} so {@link FlinkLifecycle} can be unit-tested
 * with a stub implementation (no real {@code confluent} CLI invocation).
 */
public interface ProcessRunner {

    Result run(List<String> command) throws IOException, InterruptedException;

    record Result(int exitCode, String stdout, String stderr) {}
}
