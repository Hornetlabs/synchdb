-- Persistent receipts, desired state, and process-independent recovery metadata.
CREATE FUNCTION synchdb_runtime_admitted(bigint) RETURNS boolean
AS 'MODULE_PATHNAME','synchdb_runtime_admitted' LANGUAGE C VOLATILE STRICT;

DO $control_catalog$
DECLARE
    ext oid; ns oid; schema_name name; r record; relation_oid oid;
    guard_oid oid; existing_trigger record;
    old_path text := pg_catalog.current_setting('search_path');
BEGIN
    PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
    SELECT e.oid,e.extnamespace,n.nspname INTO STRICT ext,ns,schema_name
      FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
     WHERE e.extname='synchdb';
    FOR r IN SELECT * FROM (VALUES
      ('synchdb_control_state', $definition$
        connector_id uuid PRIMARY KEY,
        connector_name name NOT NULL,
        config_revision bigint NOT NULL CHECK(config_revision>0),
        execution_role oid NOT NULL,
        runtime_incarnation bigint NOT NULL DEFAULT 0,
        desired_state text NOT NULL CHECK(desired_state IN ('RUNNING','PAUSED','STOPPED')),
        snapshot_mode text NOT NULL,
        snapshot_engine integer NOT NULL,
        schema_sync boolean NOT NULL,
        oracle_compatible boolean NOT NULL,
        run_options jsonb NOT NULL,
        retry_enabled boolean NOT NULL DEFAULT false,
        retry_attempt integer NOT NULL DEFAULT 0,
        retry_at timestamptz,
        actual_state text NOT NULL DEFAULT 'STOPPED',
        last_operation bigint NOT NULL,
        last_error text,
        last_sqlstate text,
        updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
      $definition$),
      ('synchdb_control_commands', $definition$
        operation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        connector_id uuid NOT NULL,
        requested_by oid NOT NULL,
        operation text NOT NULL CHECK(operation IN
          ('START','STOP','PAUSE','RESUME','RESTART','RELOAD','MEMDUMP','LOGLEVEL','OFFSET','RESET_STATS')),
        status text NOT NULL DEFAULT 'PENDING' CHECK(status IN
          ('PENDING','EXECUTING','SUCCEEDED','FAILED','CANCELLED')),
        config_revision bigint NOT NULL,
        payload text,
        controller_generation bigint,
        request_id bigint,
        run_generation bigint,
        sqlstate text,
        message text,
        created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
        started_at timestamptz,
        finished_at timestamptz
      $definition$)
    ) x(relation_name, definition)
    LOOP
        SELECT c.oid INTO relation_oid FROM pg_catalog.pg_class c
         WHERE c.relnamespace=ns AND c.relname=r.relation_name;
        IF relation_oid IS NULL THEN
            EXECUTE pg_catalog.format('CREATE TABLE %I.%I (%s)',schema_name,r.relation_name,r.definition);
        ELSIF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_depend d
          WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=relation_oid
            AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
            AND d.refobjid=ext AND d.deptype='e') THEN
            RAISE EXCEPTION 'Control catalog % is not an extension member',r.relation_name USING ERRCODE='55000';
        END IF;
        EXECUTE pg_catalog.format('REVOKE ALL ON TABLE %I.%I FROM PUBLIC',schema_name,r.relation_name);
    END LOOP;
    EXECUTE pg_catalog.format('CREATE UNIQUE INDEX IF NOT EXISTS synchdb_control_one_pending '
      'ON %I.synchdb_control_commands(connector_id) WHERE status IN (''PENDING'',''EXECUTING'')',schema_name);
    -- The invoker APIs write only these two ledgers in a short trusted C
    -- scope. A table grant must never become permission to forge a worker's
    -- execution role, its accepted options, command identity or receipt.
    SELECT p.oid INTO guard_oid FROM pg_catalog.pg_proc p
     WHERE p.pronamespace=ns AND p.proname='synchdb_control_ledger_guard' AND p.pronargs=0;
    IF guard_oid IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_depend d
       WHERE d.classid='pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid=guard_oid
         AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
         AND d.refobjid=ext AND d.deptype='e') THEN
        RAISE EXCEPTION 'Control ledger guard must belong to the synchdb extension'
            USING ERRCODE='55000';
    END IF;
    IF guard_oid IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_proc p WHERE p.oid=guard_oid
       AND p.prokind='f' AND p.prorettype='pg_catalog.trigger'::pg_catalog.regtype
       AND NOT p.proretset) THEN
        RAISE EXCEPTION 'Unexpected control ledger guard function signature'
            USING ERRCODE='55000';
    END IF;
    EXECUTE pg_catalog.format($function$
      CREATE OR REPLACE FUNCTION %I.synchdb_control_ledger_guard() RETURNS pg_catalog.trigger
      LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog
      AS $guard$
      BEGIN
          IF TG_LEVEL <> 'STATEMENT' OR TG_WHEN <> 'BEFORE'
             OR TG_OP NOT IN ('INSERT','UPDATE','DELETE','TRUNCATE') THEN
              RAISE EXCEPTION 'Control ledger guard requires a BEFORE statement trigger'
                  USING ERRCODE='0A000';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                          WHERE rolname=CURRENT_USER AND rolsuper) THEN
              RAISE EXCEPTION 'Control ledgers can only be modified by the trusted control API or a superuser'
                  USING ERRCODE='42501';
          END IF;
          RETURN NULL;
      END;
      $guard$
    $function$,schema_name);
    SELECT p.oid INTO STRICT guard_oid FROM pg_catalog.pg_proc p
     WHERE p.pronamespace=ns AND p.proname='synchdb_control_ledger_guard' AND p.pronargs=0;
    FOR r IN SELECT relation_name FROM (VALUES
      ('synchdb_control_state'),('synchdb_control_commands')) x(relation_name)
    LOOP
        SELECT c.oid INTO STRICT relation_oid FROM pg_catalog.pg_class c
         WHERE c.relnamespace=ns AND c.relname=r.relation_name;
        SELECT t.* INTO existing_trigger FROM pg_catalog.pg_trigger t
         WHERE t.tgrelid=relation_oid AND t.tgname='synchdb_control_ledger_guard';
        IF FOUND AND (existing_trigger.tgfoid <> guard_oid OR existing_trigger.tgtype <> 62
          OR existing_trigger.tgisinternal OR existing_trigger.tgconstraint <> 0
          OR existing_trigger.tgdeferrable OR existing_trigger.tginitdeferred
          OR existing_trigger.tgnargs <> 0 OR existing_trigger.tgattr::pg_catalog.text <> ''
          OR existing_trigger.tgqual IS NOT NULL OR existing_trigger.tgparentid <> 0
          OR existing_trigger.tgoldtable IS NOT NULL OR existing_trigger.tgnewtable IS NOT NULL) THEN
            RAISE EXCEPTION 'Unexpected control ledger guard trigger definition'
                USING ERRCODE='55000';
        END IF;
        EXECUTE pg_catalog.format('CREATE OR REPLACE TRIGGER synchdb_control_ledger_guard '
          'BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I.%I '
          'FOR EACH STATEMENT EXECUTE FUNCTION %I.synchdb_control_ledger_guard()',
          schema_name,r.relation_name,schema_name);
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER synchdb_control_ledger_guard',
          schema_name,r.relation_name);
    END LOOP;
    -- Include never-started configurations. A worker PID is not an intent/status substitute.
    EXECUTE pg_catalog.format($view$
      CREATE OR REPLACE VIEW %1$I.synchdb_control_view WITH (security_invoker=true) AS
      SELECT c.name,c.connector_id,c.config_revision AS current_revision,
             s.config_revision AS accepted_revision,s.execution_role,s.desired_state,
             CASE WHEN s.runtime_incarnation IS NOT NULL
                       AND NOT %1$I.synchdb_runtime_admitted(s.runtime_incarnation)
                  THEN 'STOPPED' ELSE coalesce(s.actual_state,'STOPPED') END
               AS actual_state,s.retry_attempt,s.retry_at,
             s.last_operation,s.last_error,s.last_sqlstate,s.updated_at
        FROM %1$I.synchdb_conninfo c
        LEFT JOIN %1$I.synchdb_control_state s USING(connector_id)
    $view$,schema_name);
    PERFORM pg_catalog.set_config('search_path',old_path,true);
END
$control_catalog$;

CREATE OR REPLACE FUNCTION synchdb_control(name,text,text DEFAULT NULL) RETURNS bigint
AS 'MODULE_PATHNAME','synchdb_control' LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION synchdb_operation(bigint,integer DEFAULT 0) RETURNS jsonb
AS 'MODULE_PATHNAME','synchdb_operation' LANGUAGE C VOLATILE;
