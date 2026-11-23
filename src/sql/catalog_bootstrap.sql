-- Local target ownership is committed in the same transaction as publication.
-- This is not an engine offset, an artifact generation, or a durable handoff.
CREATE TABLE IF NOT EXISTS synchdb_bootstrap_targets (
    target_oid       pg_catalog.oid PRIMARY KEY,
    connector_id     pg_catalog.uuid NOT NULL,
    generation       pg_catalog.uuid NOT NULL,
    source_identity  pg_catalog.jsonb NOT NULL,
    target_schema    pg_catalog.text COLLATE pg_catalog."C" NOT NULL,
    target_name      pg_catalog.text COLLATE pg_catalog."C" NOT NULL,
    target_owner     pg_catalog.oid NOT NULL,
    descriptor       pg_catalog.jsonb NOT NULL,
    descriptor_hash  pg_catalog.text NOT NULL,
    plan             pg_catalog.jsonb NOT NULL,
    published_at     pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CHECK (pg_catalog.jsonb_typeof(source_identity) = 'object'),
    CHECK (pg_catalog.jsonb_typeof(descriptor) = 'object'),
    CHECK (pg_catalog.jsonb_typeof(plan) = 'object'),
    CHECK (connector_id <> '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid)
);

-- A committed local outcome remains inspectable after its worker exits.  This
-- journal contains no engine-owned artifact/offset ownership transition.
CREATE TABLE IF NOT EXISTS synchdb_bootstrap_runs (
    generation       pg_catalog.uuid PRIMARY KEY,
    connector_id     pg_catalog.uuid NOT NULL,
    config_revision  pg_catalog.int8 NOT NULL CHECK (config_revision > 0),
    result           pg_catalog.jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(result)='object'),
    finished_at      pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
);

DO $synchdb_bootstrap_catalog$
DECLARE
    v_extension pg_catalog.oid;
    v_namespace pg_catalog.oid;
    v_relation pg_catalog.oid;
    v_table pg_catalog.text;
BEGIN
    SELECT e.oid, e.extnamespace INTO STRICT v_extension, v_namespace
      FROM pg_catalog.pg_extension e WHERE e.extname = 'synchdb';
    FOREACH v_table IN ARRAY ARRAY['synchdb_bootstrap_targets','synchdb_bootstrap_runs'] LOOP
    SELECT c.oid INTO STRICT v_relation FROM pg_catalog.pg_class c
     WHERE c.relnamespace = v_namespace AND c.relname = v_table
       AND c.relkind = 'r';
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                      AND d.objid = v_relation AND d.objsubid = 0
                      AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                      AND d.refobjid = v_extension AND d.deptype = 'e') THEN
        RAISE EXCEPTION 'Bootstrap target ownership catalog must belong to synchdb'
            USING ERRCODE = '55000';
    END IF;
    END LOOP;
END;
$synchdb_bootstrap_catalog$;

REVOKE ALL ON synchdb_bootstrap_targets FROM PUBLIC;

REVOKE ALL ON synchdb_bootstrap_runs FROM PUBLIC;
