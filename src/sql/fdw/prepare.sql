CREATE OR REPLACE FUNCTION read_snapshot_table_list(
    file_uri    text,
    p_conn_type  text,
    p_desired_db text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    file_path    text;
    file_content text;
    json_data    jsonb;
    table_list   text;
    v_is_mysql   boolean;
BEGIN
    v_is_mysql := (lower(coalesce(p_conn_type, '')) = 'mysql');

    -- strip the leading "file:" prefix
    IF position('file:' IN file_uri) = 1 THEN
        file_path := substr(file_uri, 6);
    ELSE
        RAISE EXCEPTION 'Invalid file URI format: % (must start with file:)', file_uri;
    END IF;

    -- read the file content
    file_content := pg_read_file(file_path);

    -- parse it as JSONB
    json_data := file_content::jsonb;

    -- Build comma-separated list, normalizing entries:
    --   mysql: db.table
    --   non-mysql: schema.table => db.schema.table
    SELECT string_agg(norm, ',')
      INTO table_list
    FROM (
      SELECT
        CASE
          WHEN v_is_mysql THEN x
          ELSE
            CASE
              WHEN array_length(regexp_split_to_array(trim(x), '\.'), 1) = 2
              THEN format('%s.%s', p_desired_db, trim(x))  -- prepend db.
              ELSE trim(x)  -- keep 3-part (or anything else) as-is
            END
        END AS norm
      FROM jsonb_array_elements_text(json_data->'snapshot_table_list') AS t(x)
      WHERE trim(x) <> ''
    ) s;

    RETURN table_list;
END;
$$;

/* added for oracle_fdw based initial snapshot */

CREATE OR REPLACE FUNCTION synchdb_prepare_initial_snapshot(
    p_connector_name name,
    p_master_key     text DEFAULT NULL      -- prefer supplying via GUC or parameter, not hardcoding
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
	v_connector   text;      -- 'oracle' | 'olr' | 'mysql' | 'postgres' | 'sqlserver' (lowercased)
    v_hostname    text;
    v_port        int;
    v_srcdb       text;   -- from data->>'srcdb'
    v_service     text;   -- v_srcdb or Oracle PDB name
    v_user        text;
    v_pwd         text;   -- decrypted password
    v_server      text;
    v_dbserver    text;   -- oracle_fdw "dbserver" option, e.g. //host:1521/SERVICE
    v_key         text;
    v_ssl_mode    text;   -- ssl_mode for postgres_fdw (from extra conninfo)
    v_ssl_cert    text;   -- FDW client certificate file path
    v_ssl_key     text;   -- FDW client private key file path
    v_ssl_rootcert text;  -- FDW CA cert path; for oracle_fdw: Oracle Wallet directory
    v_ssl_cipher  text;   -- SSL cipher list (mysql_fdw only)
BEGIN
    ----------------------------------------------------------------------
    -- 0) Fetch connector type and ensure we have a master key
    ----------------------------------------------------------------------
    SELECT lower(data->>'connector')
      INTO v_connector
    FROM synchdb_conninfo
    WHERE name = p_connector_name;
	
	IF v_connector IS NULL OR v_connector = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data->>connector is missing/empty', p_connector_name;
    END IF;
	
	v_key := p_master_key;
    IF v_key IS NULL OR v_key = '' THEN
        RAISE EXCEPTION 'Master key not provided.';
    END IF;

    ----------------------------------------------------------------------
    -- 1) Ensure required extensions exist (oracle_fdw and pgcrypto)
    ----------------------------------------------------------------------
	IF v_connector IN ('oracle','olr') THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'oracle_fdw') THEN
			RAISE NOTICE 'oracle_fdw not found; attempting CREATE EXTENSION';
			BEGIN
				EXECUTE 'CREATE EXTENSION oracle_fdw';
			EXCEPTION WHEN OTHERS THEN
				RAISE EXCEPTION 'Failed to install oracle_fdw: % [%]', SQLERRM, SQLSTATE
					USING HINT = 'Install oracle_fdw (and Oracle client libs) as a superuser, then retry.';
			END;
		END IF;
	ELSIF v_connector = 'mysql' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'mysql_fdw') THEN
            RAISE NOTICE 'mysql_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION mysql_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install mysql_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install mysql_fdw as a superuser, then retry.';
            END;
        END IF;
	ELSIF v_connector = 'postgres' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgres_fdw') THEN
            RAISE NOTICE 'postgres_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION postgres_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install postgres_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install postgres_fdw as a superuser, then retry.';
            END;
        END IF;
	ELSIF v_connector = 'sqlserver' THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'tds_fdw') THEN
            RAISE NOTICE 'tds_fdw not found; attempting CREATE EXTENSION';
            BEGIN
                EXECUTE 'CREATE EXTENSION tds_fdw';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to install tds_fdw: % [%]', SQLERRM, SQLSTATE
                    USING HINT = 'Install tds_fdw (and FreeTDS client libs) as a superuser, then retry.';
            END;
        END IF;
	END IF;

    ----------------------------------------------------------------------
    -- 2) Fetch connector info and DECRYPT password
    ----------------------------------------------------------------------
    SELECT
        data->>'hostname',
        NULLIF(data->>'port','')::int,
        lower(data->>'srcdb'),          -- service/SID
        data->>'user',
        pgp_sym_decrypt((data->>'pwd')::bytea, v_key)
    INTO v_hostname, v_port, v_srcdb, v_user, v_pwd
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No row in synchdb_conninfo for connector name %', p_connector_name;
    END IF;

    -- Fetch FDW TLS cert paths set via synchdb_add_fdw_conninfo (NULL when absent)
    SELECT
        NULLIF(NULLIF(data->>'ssl_mode', ''), 'null'),
        NULLIF(data->>'fdw_ssl_cert',     'null'),
        NULLIF(data->>'fdw_ssl_key',      'null'),
        NULLIF(data->>'fdw_ssl_rootcert', 'null'),
        NULLIF(pgp_sym_decrypt((data->>'fdw_ssl_cipher')::bytea, v_key), 'null')
    INTO v_ssl_mode, v_ssl_cert, v_ssl_key, v_ssl_rootcert, v_ssl_cipher
    FROM synchdb_conninfo
    WHERE name = p_connector_name;

    IF v_hostname IS NULL OR v_hostname = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.hostname is missing', p_connector_name;
    END IF;
    IF v_srcdb IS NULL OR v_srcdb = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.srcdb (service/SID) is missing', p_connector_name;
    END IF;
    -- Parse CDB/PDB format: "CDB/PDB"
    -- If srcdb contains a slash, the left part is the CDB service (used by Debezium in C code),
    -- and the right part is the PDB service used for the FDW connection here.
    IF position('/' IN v_srcdb) > 0 THEN
        v_service := split_part(v_srcdb, '/', 2);
        IF v_service = '' THEN
            RAISE EXCEPTION 'synchdb_conninfo[%]: data.srcdb has a trailing slash but no PDB service name (format: CDB/PDB)', p_connector_name;
        END IF;
        RAISE NOTICE 'CDB/PDB mode: srcdb=%, FDW will connect to PDB service "%"', v_srcdb, v_service;
    ELSE
        v_service := v_srcdb;
    END IF;

    IF v_user IS NULL OR v_user = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: data.user is missing', p_connector_name;
    END IF;
    IF v_pwd IS NULL OR v_pwd = '' THEN
        RAISE EXCEPTION 'synchdb_conninfo[%]: decrypted password is empty or invalid; check key and ciphertext', p_connector_name;
    END IF;

    v_port := COALESCE(v_port, 1521);
	v_server := format('%s_%s', p_connector_name, v_connector);	

    ----------------------------------------------------------------------
    -- 3) Recreate FDW server and user mapping with fresh info
    ----------------------------------------------------------------------
    EXECUTE format('DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER %I', v_server);
    EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', v_server);

	IF v_connector IN ('oracle','olr') THEN
		-- oracle_fdw uses Easy Connect Plus for TLS. fdw_ssl_rootcert is treated as
		-- the Oracle Wallet directory. fdw_ssl_cert/fdw_ssl_key have no Easy Connect
		-- equivalent and must be imported into the Wallet beforehand via orapki.
		IF v_ssl_rootcert IS NOT NULL THEN
			v_dbserver := format('tcps://%s:%s/%s?wallet_location=%s&ssl_server_dn_match=yes',
			                     v_hostname, v_port, v_service, v_ssl_rootcert);
			IF v_ssl_cert IS NOT NULL OR v_ssl_key IS NOT NULL THEN
				RAISE NOTICE
					'fdw_ssl_cert/fdw_ssl_key are ignored for oracle_fdw: import them into '
					'the Wallet at % using orapki or openssl pkcs12.',
					v_ssl_rootcert;
			END IF;
		ELSE
			v_dbserver := format('//%s:%s/%s', v_hostname, v_port, v_service);
		END IF;

		EXECUTE format(
			'CREATE SERVER %I FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver %L)',
			v_server, v_dbserver
		);

		EXECUTE format(
			'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
			v_server, v_user, v_pwd
		);

		RAISE NOTICE 'Created server % and user mapping for CURRENT_USER', v_server;
	ELSIF v_connector = 'mysql' THEN
		EXECUTE (
            format('CREATE SERVER %I FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host %L, port %L',
                   v_server, v_hostname, v_port::text)
            || CASE WHEN v_ssl_cert     IS NOT NULL THEN format(', ssl_cert %L',   v_ssl_cert)     ELSE '' END
            || CASE WHEN v_ssl_key      IS NOT NULL THEN format(', ssl_key %L',    v_ssl_key)      ELSE '' END
            || CASE WHEN v_ssl_rootcert IS NOT NULL THEN format(', ssl_ca %L',     v_ssl_rootcert) ELSE '' END
            || CASE WHEN v_ssl_cipher   IS NOT NULL THEN format(', ssl_cipher %L', v_ssl_cipher)   ELSE '' END
            || ')'
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (username %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSIF v_connector = 'postgres' THEN
		EXECUTE (
            format('CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, dbname %L, port %L',
                   v_server, v_hostname, v_srcdb, v_port::text)
            || CASE WHEN v_ssl_mode     IS NOT NULL THEN format(', sslmode %L',     v_ssl_mode)     ELSE '' END
            || CASE WHEN v_ssl_cert     IS NOT NULL THEN format(', sslcert %L',     v_ssl_cert)     ELSE '' END
            || CASE WHEN v_ssl_key      IS NOT NULL THEN format(', sslkey %L',      v_ssl_key)      ELSE '' END
            || CASE WHEN v_ssl_rootcert IS NOT NULL THEN format(', sslrootcert %L', v_ssl_rootcert) ELSE '' END
            || ')'
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSIF v_connector = 'sqlserver' THEN
		-- tds_fdw has no SSL-related server options; encryption (if any) is configured
		-- via FreeTDS itself (freetds.conf), not through CREATE SERVER OPTIONS.
		IF v_ssl_mode IS NOT NULL OR v_ssl_cert IS NOT NULL OR v_ssl_key IS NOT NULL OR v_ssl_rootcert IS NOT NULL THEN
			RAISE NOTICE 'fdw_ssl_* options are ignored for tds_fdw: configure TLS via FreeTDS (freetds.conf) instead.';
		END IF;

		EXECUTE format(
            'CREATE SERVER %I FOREIGN DATA WRAPPER tds_fdw OPTIONS (servername %L, port %L, database %L)',
            v_server, v_hostname, v_port::text, v_srcdb
        );

        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (username %L, password %L)',
            v_server, v_user, v_pwd
        );
	ELSE
        RAISE EXCEPTION 'Unsupported connector type: %', v_connector;
    END IF;
    RETURN v_server;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'synchdb_prepare_initial_snapshot(%) failed: % [%]',
                        p_connector_name, SQLERRM, SQLSTATE
            USING HINT = 'Verify designated FDWs are available and synchdb_conninfo JSON fields (hostname, port, srcdb, user, pwd) are valid; also ensure the master key is correct.';
END;
$$;

COMMENT ON FUNCTION synchdb_prepare_initial_snapshot(name, text) IS
   'check oracle_fdw and prepare foreign server and user mapping objects';

