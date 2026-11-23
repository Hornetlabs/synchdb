package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.HistoryRecord;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

/* Read-only preflight. A broken engine generation must not become an implicit
 * fresh snapshot merely because an engine's backing-store loader tolerates it. */
final class ArtifactFiles
{
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private ArtifactFiles() {}

    static void validateHistory(int connectorType, Path path)
    {
        validateHistory(connectorType,path,!Files.notExists(path,LinkOption.NOFOLLOW_LINKS));
    }

    static void validateHistory(int connectorType, Path path, boolean resumeExisting)
    {
        if (connectorType == 5)
            return;
        if (!resumeExisting)
        {
            if (!Files.notExists(path,LinkOption.NOFOLLOW_LINKS))
                throw new IllegalStateException("A fresh artifact generation cannot adopt existing schema history");
            return;
        }
        // A resume must open its required history. Do not turn a file removed
        // after the C ownership check into an implicit empty backing store.
        try
        {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > 64L * 1024 * 1024)
                throw new IOException("History must be a regular file no larger than 64 MiB");
            long count = 0;
            try (BufferedReader input = Files.newBufferedReader(path, StandardCharsets.UTF_8))
            {
                String line;
                while ((line = input.readLine()) != null)
                {
                    if (line.isBlank())
                        throw new IOException("Blank history record");
                    if (!JSON.readTree(line).isObject() ||
                        !new HistoryRecord(DocumentReader.defaultReader().read(line)).isValid())
                        throw new IOException("Invalid Debezium history record");
                    count++;
                }
            }
            if (count == 0)
                throw new IOException("Empty engine-owned history");
        }
        catch (IOException failure)
        {
            throw new IllegalStateException("Cannot validate engine-owned history " + path, failure);
        }
    }
}
