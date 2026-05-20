package com.example.kf2sql;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Production {@link ProcessRunner} that invokes commands via {@link ProcessBuilder}.
 *
 * <p>stdout and stderr are captured to memory. For long-running statements
 * (e.g. {@code statement create --wait}) this is fine because CC's CLI itself
 * blocks until terminal state — we don't need streaming.
 */
public class RealProcessRunner implements ProcessRunner {

    @Override
    public Result run(List<String> command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(false);
        Process proc = pb.start();
        String stdout = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(proc.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = proc.waitFor();
        return new Result(exit, stdout, stderr);
    }
}
