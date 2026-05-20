package com.example.kf2sql.quarkus.config;

import com.example.kf2sql.quarkus.SqlGenerator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * CDI producer for SqlGenerator. Reads the SQL template from the configured path
 * at startup and creates a singleton SqlGenerator bean.
 */
@ApplicationScoped
public class SqlGeneratorProducer {

    @ConfigProperty(name = "kf.sql-template-path")
    String templatePath;

    @Produces
    @ApplicationScoped
    public SqlGenerator sqlGenerator() {
        try {
            String template = Files.readString(Path.of(templatePath));
            return new SqlGenerator(template);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read SQL template: " + templatePath, e);
        }
    }
}
