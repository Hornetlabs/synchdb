-- Key material belongs only to the configured server keyring. These functions
-- are invoker APIs; table SELECT/RLS and pgcrypto ACLs continue to apply.
CREATE OR REPLACE FUNCTION synchdb_connector_credential(pg_catalog.text,pg_catalog.text)
RETURNS pg_catalog.text
AS 'MODULE_PATHNAME','synchdb_connector_credential'
LANGUAGE C VOLATILE STRICT SECURITY INVOKER;
CREATE OR REPLACE FUNCTION synchdb_reencrypt_credentials()
RETURNS pg_catalog.int8
AS 'MODULE_PATHNAME','synchdb_reencrypt_credentials'
LANGUAGE C VOLATILE SECURITY INVOKER;
REVOKE ALL ON FUNCTION synchdb_connector_credential(pg_catalog.text,pg_catalog.text),
    synchdb_reencrypt_credentials() FROM PUBLIC;
