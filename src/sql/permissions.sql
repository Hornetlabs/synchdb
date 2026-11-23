-- New installations expose APIs only through explicit grants. Resolve members
-- through extension dependency, including relocated extensions.
DO $synchdb_permissions$
DECLARE
    f record;
BEGIN
    FOR f IN
        SELECT p.oid::pg_catalog.regprocedure AS signature,
               l.lanname AS language_name
          FROM pg_catalog.pg_proc p
          JOIN pg_catalog.pg_language l ON l.oid = p.prolang
          JOIN pg_catalog.pg_depend d
            ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
           AND d.objid = p.oid AND d.objsubid = 0
           AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND d.deptype = 'e'
          JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
         WHERE e.extname = 'synchdb'
    LOOP
        IF f.language_name = 'c' THEN
            EXECUTE pg_catalog.format('ALTER FUNCTION %s VOLATILE', f.signature);
        END IF;
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f.signature);
    END LOOP;
END
$synchdb_permissions$;
