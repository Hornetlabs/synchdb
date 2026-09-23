CREATE OR REPLACE FUNCTION oracle_type_to_jdbc(ora_type_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  t    text;      -- normalized input
  base text;      -- base type token(s)
  m    text[];    -- regex match for (...) args
BEGIN
  -- Normalize: trim, compress spaces, uppercase, strip quotes
  t := regexp_replace(upper(btrim(ora_type_text)), '\s+', ' ', 'g');
  t := replace(replace(t, '"', ''), '''', '');

  -- Multi-word TZ types FIRST (match full text)
  IF t LIKE 'TIMESTAMP% WITH TIME ZONE%' THEN
    RETURN -101;     -- OracleTypes.TIMESTAMPTZ
  ELSIF t LIKE 'TIMESTAMP% WITH LOCAL TIME ZONE%' THEN
    RETURN -102;     -- OracleTypes.TIMESTAMPLTZ
  ELSIF t LIKE 'INTERVAL YEAR% TO MONTH%' THEN
    RETURN -103;     -- OracleTypes.INTERVALYM
  ELSIF t LIKE 'INTERVAL DAY% TO SECOND%' THEN
    RETURN -104;     -- OracleTypes.INTERVALDS
  END IF;

  -- Extract base token before any ( ... ) or trailing qualifiers
  m := regexp_match(t, '^([A-Z ]+)\s*\(');
  IF m IS NOT NULL THEN
    base := btrim(m[1]);
  ELSE
    base := t;
  END IF;

  -- Switch on base (single-word & simple multi-word that remain)
  CASE base
    -- Numeric family (Debezium sets NUMBER/DECIMAL/NUMERIC → Types.NUMERIC)
    WHEN 'NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER', 'SMALLINT' THEN
      RETURN 2;          -- java.sql.Types.NUMERIC

    -- Floating family
    WHEN 'BINARY_FLOAT' THEN
      RETURN 100;        -- OracleTypes.BINARY_FLOAT
    WHEN 'BINARY_DOUBLE' THEN
      RETURN 101;        -- OracleTypes.BINARY_DOUBLE
    WHEN 'FLOAT' THEN
      RETURN 6;          -- java.sql.Types.FLOAT
    WHEN 'DOUBLE' THEN
      -- In Debezium grammar DOUBLE PRECISION maps to FLOAT (length set separately)
      RETURN 6;          -- java.sql.Types.FLOAT
    WHEN 'REAL' THEN
      RETURN 6;          -- Debezium maps REAL → FLOAT too

    -- Date/Time (Oracle JDBC reports DATE as TIMESTAMP)
    WHEN 'DATE' THEN
      RETURN 93;         -- java.sql.Types.TIMESTAMP
    WHEN 'TIMESTAMP' THEN
      RETURN 93;         -- java.sql.Types.TIMESTAMP

    -- Character family
    WHEN 'CHAR', 'CHARACTER' THEN
      RETURN 1;          -- CHAR
    WHEN 'NCHAR' THEN
      RETURN -15;        -- NCHAR
    WHEN 'VARCHAR2', 'VARCHAR' THEN
      RETURN 12;         -- VARCHAR
    WHEN 'NVARCHAR2', 'NVARCHAR' THEN
      RETURN -9;         -- NVARCHAR
	WHEN 'LONG' THEN
	  RETURN -1;         -- LONG

    -- LOBs
    WHEN 'BLOB' THEN
      RETURN 2004;       -- BLOB
    WHEN 'CLOB' THEN
      RETURN 2005;       -- CLOB
    WHEN 'NCLOB' THEN
      RETURN 2011;       -- NCLOB
    WHEN 'BFILE' THEN
      RETURN -13;        -- OracleTypes.BFILE

    -- Binary
    WHEN 'RAW' THEN
      RETURN -3;         -- OracleTypes.RAW (maps to BINARY)
    WHEN 'LONG RAW' THEN
      RETURN -4;         -- LONGVARBINARY

    -- Rowid (Debezium code sets ROWID → VARCHAR)
    WHEN 'ROWID', 'UROWID' THEN
      RETURN -8;         -- VARCHAR

    -- XML / Spatial / Other
    WHEN 'XMLTYPE' THEN
      RETURN 2009;       -- SQLXML (fall back to 1111 if your driver lacks it)
    WHEN 'SDO_GEOMETRY' THEN
      RETURN 1111;       -- OTHER

    -- 23ai BOOLEAN (if encountered)
    WHEN 'BOOLEAN' THEN
      RETURN 16;         -- java.sql.Types.BOOLEAN

    ELSE
      RETURN 1111;       -- OTHER (catch-all for unrecognized)
  END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION mysql_type_to_jdbc(mysql_type_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  t           text;     -- normalized input
  base        text;     -- base type token(s) without length/precision
  m           text[];   -- regex match for (...) args
  is_unsigned boolean := false;
BEGIN
  -- Normalize: trim, compress spaces, uppercase, strip quotes/backticks
  t := regexp_replace(upper(btrim(mysql_type_text)), '\s+', ' ', 'g');
  t := replace(replace(t, '"', ''), '''', '');
  t := replace(t, '`', '');

  -- Detect UNSIGNED flag (doesn't usually affect jdbcType, but matters for BIGINT)
  IF t LIKE '% UNSIGNED' THEN
    is_unsigned := true;
    t := regexp_replace(t, '\s+UNSIGNED$', '');
  END IF;

  -- Extract base token before any ( ... ) or trailing qualifiers
  m := regexp_match(t, '^([A-Z0-9_ ]+)\s*\(');
  IF m IS NOT NULL THEN
    base := btrim(m[1]);
  ELSE
    base := t;
  END IF;

  /*
   * Map to java.sql.Types (int) in a Debezium-like way.
   *
   * Reference (commonly used values):
   *   CHAR            -> 1
   *   VARCHAR/TEXT    -> 12
   *   BIT             -> -7
   *   TINYINT/SMALLINT-> 5
   *   INTEGER         -> 4
   *   BIGINT          -> -5 (or 3 when UNSIGNED to avoid overflow)
   *   NUMERIC/DECIMAL -> 3
   *   REAL/FLOAT      -> 6
   *   DOUBLE          -> 8
   *   DATE            -> 91
   *   TIME            -> 92
   *   TIMESTAMP       -> 2014 (timestamp with time zone, as in your example)
   *   BINARY          -> -2
   *   VARBINARY       -> -3
   *   BLOB*           -> 2004
   *   JSON/GEOMETRY   -> 1111 (OTHER)
   */

  CASE base

    ------------------------------------------------------------------
    -- Exact numeric / decimal family
    ------------------------------------------------------------------
    WHEN 'DECIMAL', 'NUMERIC', 'FIXED' THEN
      RETURN 3;       -- DECIMAL (signed or unsigned; length/scale handled elsewhere)

    ------------------------------------------------------------------
    -- Float / double family
    ------------------------------------------------------------------
    WHEN 'DOUBLE', 'DOUBLE PRECISION' THEN
      RETURN 8;       -- DOUBLE
    WHEN 'REAL', 'FLOAT' THEN
      RETURN 6;       -- FLOAT

    ------------------------------------------------------------------
    -- Integer family
    ------------------------------------------------------------------
    WHEN 'BIGINT' THEN
      -- Debezium uses DECIMAL for BIGINT UNSIGNED to avoid overflow.
      IF is_unsigned THEN
        RETURN 3;     -- DECIMAL
      ELSE
        RETURN -5;    -- BIGINT
      END IF;

    WHEN 'INT', 'INTEGER', 'MEDIUMINT' THEN
      RETURN 4;       -- INTEGER

    WHEN 'SMALLINT', 'TINYINT', 'YEAR' THEN
      -- Your example shows TINYINT and YEAR as int/short-ish types
      RETURN 5;       -- SMALLINT

    ------------------------------------------------------------------
    -- Bit / boolean
    ------------------------------------------------------------------
    WHEN 'BIT' THEN
      RETURN -7;      -- BIT

    WHEN 'BOOL', 'BOOLEAN' THEN
      RETURN 16;      -- BOOLEAN (not from your example, but sane to support)

    ------------------------------------------------------------------
    -- Character / text
    ------------------------------------------------------------------
    WHEN 'CHAR', 'NCHAR' THEN
      RETURN 1;       -- CHAR

    WHEN 'VARCHAR', 'NVARCHAR',
         'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT',
         'ENUM', 'SET' THEN
      -- Debezium maps all these to VARCHAR-ish
      RETURN 12;      -- VARCHAR

    ------------------------------------------------------------------
    -- Date / time
    ------------------------------------------------------------------
    WHEN 'DATE' THEN
      RETURN 91;      -- DATE

    WHEN 'TIME' THEN
      RETURN 92;      -- TIME (with or without fractional seconds; length handled separately)

    WHEN 'DATETIME' THEN
      RETURN 93;      -- TIMESTAMP

    WHEN 'TIMESTAMP' THEN
      -- In your example, TIMESTAMP maps to 2014 (TIMESTAMP_WITH_TIMEZONE)
      RETURN 2014;

    ------------------------------------------------------------------
    -- Binary / blobs
    ------------------------------------------------------------------
    WHEN 'BINARY' THEN
      RETURN -2;      -- BINARY

    WHEN 'VARBINARY' THEN
      RETURN -3;      -- VARBINARY

    WHEN 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB' THEN
      RETURN 2004;    -- BLOB

    ------------------------------------------------------------------
    -- JSON and spatial types
    ------------------------------------------------------------------
    WHEN 'JSON' THEN
      RETURN 1111;    -- OTHER

    WHEN 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON',
         'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON',
         'GEOMETRYCOLLECTION' THEN
      RETURN 1111;    -- OTHER

    ------------------------------------------------------------------
    -- Fallback
    ------------------------------------------------------------------
    ELSE
      RETURN 1111;    -- OTHER (catch-all)
  END CASE;
END;
$$;

CREATE OR REPLACE FUNCTION synchdb_type_to_jdbc(
    p_connector text,
    p_type_text text
)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    v_conn text := lower(p_connector);
BEGIN
    CASE v_conn
        WHEN 'oracle', 'olr' THEN
            RETURN oracle_type_to_jdbc(p_type_text);

        WHEN 'mysql' THEN
            RETURN mysql_type_to_jdbc(p_type_text);
        ELSE
            RAISE EXCEPTION
                'synchdb_type_to_jdbc: unsupported connector type % for type %',
                p_connector, p_type_text;
    END CASE;
END;
$$;
