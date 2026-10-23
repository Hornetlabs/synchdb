package com.example;

/** Metadata-only producer which never opens a second CDC engine. */
public final class OlrSchemaSnapshotter extends io.debezium.snapshot.mode.NoDataSnapshotter
{
    @Override public String name() { return "synchdb_olr_schema"; }
    @Override public boolean shouldStream() { return false; }
}
