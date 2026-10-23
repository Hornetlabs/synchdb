-- Native engine state survives worker restarts and connector configuration
-- deletion. UUID generations are separate from transient runtime attempts.
CREATE TABLE IF NOT EXISTS synchdb_artifacts (
    database_oid pg_catalog.oid NOT NULL,
    connector_id pg_catalog.uuid NOT NULL,
    generation pg_catalog.uuid NOT NULL,
    config_revision pg_catalog.int8 NOT NULL CHECK (config_revision > 0),
    source_identity pg_catalog.jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(source_identity) = 'object'),
    phase pg_catalog.text NOT NULL CHECK (phase IN ('ENGINE_OWNED','PURGE_REQUESTED','PURGING','PURGE_FAILED','PURGED')),
    active pg_catalog.bool NOT NULL,
    origin pg_catalog.text NOT NULL CHECK (origin IN ('NATIVE','LEGACY_COPY')),
    format_version pg_catalog.int4 NOT NULL DEFAULT 1 CHECK (format_version = 1),
    created_at pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    updated_at pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    last_error pg_catalog.text,
    last_sqlstate pg_catalog.text,
    PRIMARY KEY (database_oid, connector_id, generation),
    CHECK (NOT active OR phase = 'ENGINE_OWNED'),
    CHECK (connector_id <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid),
    CHECK (generation <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid)
);
CREATE UNIQUE INDEX IF NOT EXISTS synchdb_artifacts_active
    ON synchdb_artifacts(database_oid,connector_id) WHERE active;
CREATE INDEX IF NOT EXISTS synchdb_artifacts_purge_pending
    ON synchdb_artifacts(database_oid,created_at)
    WHERE phase IN ('PURGE_REQUESTED','PURGING');

DO $synchdb_artifact_catalog$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_relation pg_catalog.oid;
BEGIN
    SELECT e.oid,e.extnamespace INTO STRICT v_extension,v_namespace
      FROM pg_catalog.pg_extension e WHERE e.extname='synchdb';
    SELECT c.oid INTO STRICT v_relation FROM pg_catalog.pg_class c
     WHERE c.relnamespace=v_namespace AND c.relname='synchdb_artifacts' AND c.relkind='r';
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                    WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass
                      AND d.objid=v_relation AND d.objsubid=0
                      AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
                      AND d.refobjid=v_extension AND d.deptype='e') THEN
        RAISE EXCEPTION 'Artifact catalog must belong to synchdb' USING ERRCODE='55000';
    END IF;
END;
$synchdb_artifact_catalog$;
REVOKE ALL ON synchdb_artifacts FROM PUBLIC;

-- The replay coordinate commits in the same WAL transaction as source data.
-- Zero snapshot_scn is PREPARED; positive snapshot_scn is CDC eligible.
CREATE TABLE synchdb_olr_positions (
    database_oid pg_catalog.oid NOT NULL,
    connector_id pg_catalog.uuid NOT NULL,
    generation pg_catalog.uuid NOT NULL,
    source_proof pg_catalog.jsonb NOT NULL,
    snapshot_scn pg_catalog.int8 NOT NULL CHECK (snapshot_scn >= 0),
    scn pg_catalog.int8 NOT NULL CHECK (scn >= 0),
    c_scn pg_catalog.int8 NOT NULL CHECK (c_scn >= 0),
    c_idx pg_catalog.int8 NOT NULL CHECK (c_idx >= 0),
    PRIMARY KEY (database_oid,connector_id,generation),
    FOREIGN KEY (database_oid,connector_id,generation)
        REFERENCES synchdb_artifacts(database_oid,connector_id,generation)
);
REVOKE ALL ON synchdb_olr_positions FROM PUBLIC;

CREATE OR REPLACE FUNCTION synchdb_artifact_manifest(pg_catalog.uuid) RETURNS pg_catalog.jsonb
AS 'MODULE_PATHNAME','synchdb_artifact_manifest' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_reset(pg_catalog.uuid) RETURNS pg_catalog.int4
AS 'MODULE_PATHNAME','synchdb_artifact_reset' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_request_purge(pg_catalog.uuid) RETURNS pg_catalog.int4
AS 'MODULE_PATHNAME','synchdb_artifact_request_purge' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION synchdb_artifact_migrate_legacy(pg_catalog.name) RETURNS pg_catalog.uuid
AS 'MODULE_PATHNAME','synchdb_artifact_migrate_legacy' LANGUAGE C VOLATILE STRICT;
REVOKE ALL ON FUNCTION synchdb_artifact_manifest(pg_catalog.uuid),
    synchdb_artifact_reset(pg_catalog.uuid),synchdb_artifact_request_purge(pg_catalog.uuid),
    synchdb_artifact_migrate_legacy(pg_catalog.name) FROM PUBLIC;
