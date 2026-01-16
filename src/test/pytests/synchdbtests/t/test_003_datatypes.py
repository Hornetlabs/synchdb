import common
import json
import time
from psycopg2.extras import NumericRange, DateRange, DateRange, DateTimeTZRange, DateTimeRange

from decimal import Decimal
from datetime import datetime, timezone, timedelta, date
from binascii import unhexlify

from common import run_pg_query, run_pg_query_one, run_remote_query, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, create_and_start_synchdb_connector, stop_and_delete_synchdb_connector, getSchema, drop_default_pg_schema, drop_repslot_and_pub, update_guc_conf

def parse_time_with_fraction(t):
    if '.' in t:
        main, frac = t.split('.')
        frac = frac[:6].ljust(6, '0')
        t = f"{main}.{frac}"
        return datetime.strptime(t, "%H:%M:%S.%f").time()
    return datetime.strptime(t, "%H:%M:%S").time()

def parse_datetime_with_fraction(t):
    if '.' in t:
        main, frac = t.split('.')
        frac = frac[:6].ljust(6, '0')
        t = f"{main}.{frac}"
        return datetime.strptime(t, "%Y-%m-%d %H:%M:%S.%f")
    return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")

def parse_datetime_with_fraction_and_tz(dtstr):
    # Split timestamp and timezone
    if '+' in dtstr:
        main, tz = dtstr.split('+')
        tz = '+' + tz.replace(':', '')
    elif '-' in dtstr[20:]:  # Avoid splitting the date part
        main, tz = dtstr.rsplit('-', 1)
        tz = '-' + tz.replace(':', '')
    else:
        main, tz = dtstr, ''

    # Truncate or pad fractional seconds to 6 digits
    if '.' in main:
        base, frac = main.split('.')
        frac = frac[:6].ljust(6, '0')
        main = f"{base}.{frac}"

    final = main + tz
    return datetime.strptime(final, "%Y-%m-%d %H:%M:%S.%f%z")

def parse_ora_day2second_interval(s):
    """
    Parses '+DD HH:MM:SS.ffffff' to timedelta
    """
    s = s.strip().lstrip('+')  # remove '+' if present
    days_part, time_part = s.split(' ')
    h, m, sec = time_part.split(':')
    seconds, _, micros = sec.partition('.')
    return timedelta(
        days=int(days_part),
        hours=int(h),
        minutes=int(m),
        seconds=int(seconds),
        microseconds=int(micros) if micros else 0
    )

def parse_ora_year2month_interval(s):
    """
    Parses Oracle-style '+YY-MM' or '-YY-MM' into timedelta, assuming:
    - 1 year = 365 days
    - 1 month = 30 days
    """
    sign = -1 if s.startswith('-') else 1
    years = int(s[1:3])
    months = int(s[4:6])
    total_days = sign * (years * 365 + months * 30)
    return timedelta(days=total_days)

def parse_timedelta(s: str):
    days, _, time = s.split()
    h, m, sec = map(int, time.split(':'))
    return timedelta(days=int(days), hours=h, minutes=m, seconds=sec)

def test_AllDefaultDataTypes(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_addt"
    dbname = getDbname(dbvendor).lower()

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

    if dbvendor == "mysql":
        query = """
        CREATE TABLE mytable (
            a DEC(20,4) unsigned,
            b decimal(10,4),
            c varchar(20),
            d fixed,
            e fixed(5,2) unsigned,
            f bit(64),
            g bool,
            h double,
            i double precision,
            j double precision unsigned,
            k double unsigned,
            l real,
            m real unsigned,
            n float,
            o float unsigned,
            p int unsigned,
            q integer unsigned,
            r mediumint,
            s mediumint unsigned,
            t year,
            u smallint,
            v smallint unsigned,
            w tinyint,
            x tinyint unsigned,
            y date,
            z datetime,
            aa time,
            bb timestamp,
            cc BINARY(16),
            dd VARBINARY(255),
            ee BLOB,
            ff LONGBLOB,
            gg TINYBLOB,
            hh long varchar,
            ii longtext,
            jj mediumtext,
            kk tinytext,
            ll JSON,
            mm int,
            nn int auto_increment,
            primary key(nn),
            oo timestamp(6),
            pp time(6));
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE mytable (
            a bit,
            b DECIMAL(20,4),
            c DECIMAL(10, 4),
            d DECIMAL(10, 2),
            e DECIMAL(5, 2),
            f bit,
            g float,
            h real,
            i int,
            j smallint,
            k tinyint,
            l bigint,
            m numeric(10,2),
            n money,
            o smallmoney,
            p date,
            q time,
            r datetime2,
            s datetimeoffset,
            t datetime,
            u smalldatetime,
            v char,
            w varchar(48),
            x text,
            y nchar,
            z nvarchar(48),
            aa ntext,
            bb BINARY(10),
            cc VARBINARY(50),
            dd IMAGE,
            ee GEOMETRY,
            ff GEOGRAPHY,
            gg NVARCHAR(2000),
            hh XML,
            ii VARBINARY(4096),
            jj UNIQUEIDENTIFIER,
            kk time(6),
            ll datetime2(6),
            mm datetimeoffset(6));
        EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',
            @source_name = 'mytable', @role_name = NULL,
            @supports_net_changes = 0;
        """
    elif dbvendor == "postgres":
        time.sleep(20)
        query = """
        CREATE TABLE mytable (
            col_smallint        SMALLINT,
            col_integer         INTEGER,
            col_bigint          BIGINT,
            col_decimal         DECIMAL(10,5),
            col_numeric         NUMERIC(20,10),
            col_real            REAL,
            col_double          DOUBLE PRECISION,
            col_smallserial     SMALLSERIAL,
            col_serial          SERIAL,
            col_bigserial       BIGSERIAL,
            col_money           MONEY,
            col_char            CHAR(10),
            col_varchar         VARCHAR(255),
            col_text            TEXT,
            col_bytea           BYTEA,
            col_boolean         BOOLEAN,
            col_date            DATE,
            col_time            TIME,
            col_timetz          TIME WITH TIME ZONE,
            col_timestamp       TIMESTAMP,
            col_timestamptz     TIMESTAMP WITH TIME ZONE,
            col_interval        INTERVAL,
            col_uuid            UUID,
            col_json            JSON,
            col_jsonb           JSONB,
            col_xml             XML,
            col_inet            INET,
            col_cidr            CIDR,
            col_macaddr         MACADDR,
            col_macaddr8        MACADDR8,
            col_tsvector        TSVECTOR,
            col_tsquery         TSQUERY,
            col_bit             BIT(8),
            col_varbit          BIT VARYING(64),
            col_int4range       INT4RANGE,
            col_int8range       INT8RANGE,
            col_numrange        NUMRANGE,
            col_daterange       DATERANGE,
            col_tsrange         TSRANGE,
            col_tstzrange       TSTZRANGE,
            col_int_array       INTEGER[],
            col_text_array      TEXT[],
            col_jsonb_array     JSONB[],
            col_point           POINT,
            col_identity        BIGINT GENERATED ALWAYS AS IDENTITY,
            col_not_null        TEXT NOT NULL,
            col_unique          TEXT UNIQUE,
            col_primary_key     INTEGER PRIMARY KEY
        )
        """
    else:
        query = """
        CREATE TABLE mytable (
            id NUMBER PRIMARY KEY,                   
            binary_double_col BINARY_DOUBLE,         
            binary_float_col BINARY_FLOAT,           
            float_col FLOAT(10),                      
            number_col NUMBER(10,2),                  
            long_col LONG,
            date_col DATE,                            
            interval_ds_col INTERVAL DAY TO SECOND,   
            interval_ym_col INTERVAL YEAR TO MONTH,   
            timestamp_col TIMESTAMP,                  
            timestamp_tz_col TIMESTAMP WITH TIME ZONE, 
            timestamp_ltz_col TIMESTAMP WITH LOCAL TIME ZONE, 
            char_col CHAR(10),                        
            nchar_col NCHAR(10),                      
            nvarchar2_col NVARCHAR2(50),              
            varchar_col VARCHAR(50),                  
            varchar2_col VARCHAR2(50),                
            raw_col RAW(100),                         
            bfile_col BFILE,                          
            blob_col BLOB,                            
            clob_col CLOB,                            
            nclob_col NCLOB,                          
            rowid_col ROWID,                          
            urowid_col UROWID);
            commit;
        """

    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(30)
    else:
        time.sleep(20)
    
    # verify default data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

    # insert a record and observe replication
    if dbvendor == "mysql":
        query = """
        INSERT INTO mytable (
            a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p,
            q, r, s, t, u, v, w, x, y, z, aa, bb, cc, dd, ee,
            ff, gg, hh, ii, jj, kk, ll, mm, oo, pp
        ) VALUES (
            12345.6789, -- a: DEC(20,4) UNSIGNED
            -12345.6789,             -- b: DECIMAL(10,4)
            'Sample Text',           -- c: VARCHAR(20)
            12345.67,                -- d: FIXED (Alias for DECIMAL)
            123.45,                  -- e: FIXED(5,2) UNSIGNED
            b'0000000100000011000001110000111100011111001111110111111100000001',     -- f: BIT(64)
            TRUE,                    -- g: BOOL
            123456.789,              -- h: DOUBLE
            98765.4321,              -- i: DOUBLE PRECISION
            54321.1234,              -- j: DOUBLE PRECISION UNSIGNED
            987654.321,              -- k: DOUBLE UNSIGNED
            12345.67,                -- l: REAL
            54321.12,                -- m: REAL UNSIGNED
            12345.67,                -- n: FLOAT
            54321.12,                -- o: FLOAT UNSIGNED
            4294967295,              -- p: INT UNSIGNED
            2147483647,              -- q: INTEGER UNSIGNED
            8388607,                 -- r: MEDIUMINT
            16777215,                -- s: MEDIUMINT UNSIGNED
            2024,                    -- t: YEAR
            -32768,                  -- u: SMALLINT
            65535,                   -- v: SMALLINT UNSIGNED
            -128,                    -- w: TINYINT
            255,                     -- x: TINYINT UNSIGNED
            '2024-08-26',            -- y: DATE
            '2024-08-26 12:34:56',   -- z: DATETIME
            '12:34:56',              -- aa: TIME
            CURRENT_TIMESTAMP,       -- bb: TIMESTAMP
            0x1234567890ABCDEF1234567890ABCDEF, -- cc: BINARY(16)
            0xDEADBEEF,              -- dd: VARBINARY(255)
            'Some blob data',        -- ee: BLOB
            'Some long blob data',   -- ff: LONGBLOB
            'Tiny blob data',        -- gg: TINYBLOB
            'Long VARCHAR data',     -- hh: LONG VARCHAR
            'This is a long text',   -- ii: LONGTEXT
            'This is a medium text', -- jj: MEDIUMTEXT
            'Tiny text',             -- kk: TINYTEXT
            '{"key": "value"}',      -- ll: JSON
            123456789,               -- mm: INT
            '2024-08-29T15:30:00.123456',
            '12:34:56.111122');
        """
    elif dbvendor == "sqlserver":
        query = """
        INSERT INTO dbo.mytable (
            a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p,
            q, r, s, t, u, v, w, x, y, z, aa, bb, cc, dd, ee,
            ff, gg, hh, ii, jj, kk, ll, mm)
        VALUES (
            1,
            12345678901234.5678,
            12345.6789,
            12345.67,
            123.45,
            0,
            12345.6789,
            123.45,
            123456,
            12345,
            123,
            1234567890123456789,
            1234567.89,
            1234567.89,
            12345.67,
            '2024-08-28',
            '14:30:00',
            '2024-08-28 14:30:00.1234567',
            '2024-08-28 14:30:00.1234567 +02:00',
            '2024-08-28 14:30:00',
            '2024-08-28 14:30:00',
            'A',
            'Sample text for varchar.',
            'This is a text field.',
            'A',
            N'Sample text for nvarchar.',
            N'This is an ntext field.',
            0x1234567890,
            0x1234567890ABCDEF,
            0x1234567890ABCDEF1234567890ABCDEF1234567890,
            GEOMETRY::STGeomFromText('POINT(1 1)', 0),
            GEOGRAPHY::STGeomFromText('POINT(1 1)', 4326),
            N'Sample long text for nvarchar(max)',
            '<root><element>Value</element></root>',
            0x1234567890ABCDEF1234567890ABCDEF1234567890,
            NEWID(),
            '13:45:50.123456',
            '2024-08-28 14:30:00.654321',
            '2024-08-28 14:30:00.654321 +06:00');
        """
    elif dbvendor == "postgres":
        query = """
        INSERT INTO mytable (
            col_smallint, col_integer, col_bigint, col_decimal, col_numeric, col_real,
            col_double, col_money, col_char, col_varchar, col_text, col_bytea, col_boolean,
            col_date, col_time, col_timetz, col_timestamp, col_timestamptz, col_interval,
            col_uuid, col_json, col_jsonb, col_xml, col_inet, col_cidr, col_macaddr,
            col_macaddr8, col_tsvector, col_tsquery, col_bit, col_varbit, col_int4range,
            col_int8range, col_numrange, col_daterange, col_tsrange, col_tstzrange, col_int_array,
            col_text_array, col_jsonb_array, col_point, col_not_null, col_unique, col_primary_key
        )
        VALUES (
            123,
            1000,
            9000000000,
            12345.67890,
            98765.4321000000,
            3.14,
            2.718281828459,
            '$1234.56',
            'ABC',
            'varchar value',
            'some long text here',
            '\\xDEADBEEF'::bytea,
            true,
            DATE '2026-01-01',
            TIME '12:34:56',
            TIME '12:34:56+02',
            TIMESTAMP '2026-01-01 12:34:56',
            TIMESTAMPTZ '2026-01-01 12:34:56+02',
            INTERVAL '3 days 4 hours',
            gen_random_uuid(),
            '{"a":1,"b":"text"}',
            '{"x":[1,2,3]}'::jsonb,
            '<root><item>value</item></root>',
            '192.168.1.10',
            '192.168.0.0/24',
            '08:00:2b:01:02:03',
            '08:00:2b:ff:fe:12:34:56',
            to_tsvector('english', 'PostgreSQL full text search'),
            to_tsquery('postgresql & search'),
            B'10101010',
            B'101010',
            '[1,10]'::int4range,
            '[10000000000,20000000000]'::int8range,
            '[1.5,9.9]'::numrange,
            '[2026-01-01,2026-01-31]'::daterange,
            '[2026-01-01 10:00,2026-01-01 12:00]'::tsrange,
            '[2026-01-01 10:00+00,2026-01-01 12:00+00]'::tstzrange,
            ARRAY[1,2,3,4],
            ARRAY['a','b','c'],
            ARRAY['{"k":1}','{"k":2}']::jsonb[],
            POINT(10.5, 20.25),
            'must not be null',
            'unique-value-1',
            1
        )
        """
    else:
        query = """
        INSERT INTO mytable (
            id, binary_double_col, binary_float_col, float_col, number_col, 
            long_col, date_col, interval_ds_col, interval_ym_col, timestamp_col, 
            timestamp_tz_col, timestamp_ltz_col, char_col, nchar_col, 
            nvarchar2_col, varchar_col, varchar2_col, raw_col, 
            bfile_col, blob_col, clob_col, nclob_col, rowid_col, urowid_col
        ) VALUES (
            1,
            12345.6789,
            1234.56,
            9876.54321,
            1000.50, 
            'This is a long text', 
            TO_DATE('2024-01-31', 'YYYY-MM-DD'), 
            INTERVAL '2 03:04:05' DAY TO SECOND, 
            INTERVAL '1-6' YEAR TO MONTH, 
            TIMESTAMP '2024-01-31 10:30:00', 
            TIMESTAMP '2024-01-31 10:30:00 -08:00', 
            SYSTIMESTAMP, 
            'A', 
            N'B', 
            N'Unicode Text', 
            'Text Data', 
            'More Text Data', 
            HEXTORAW('DEADBEEF'), 
            BFILENAME('MY_DIR', 'file.pdf'),
            TO_BLOB(HEXTORAW('DEADBEEF')), 
            TO_CLOB('This is a non-empty CLOB text data'),
            TO_NCLOB('This is a non-empty NCLOB text data'),
            NULL, 
            NULL);
        COMMIT;
        """

    run_remote_query(dbvendor, query)
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(15)

    rows = run_pg_query(pg_cursor, f"SELECT * FROM {dbname}.mytable")
    if dbvendor == "mysql":
        extrows = run_remote_query(dbvendor, f"""
                SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p,
                q, r, s, t, u, v, w, x, y, z, aa, bb, HEX(cc), HEX(dd), ee,
                ff, gg, hh, ii, jj, kk, ll, mm, oo, pp FROM mytable
                """)
    elif dbvendor == "sqlserver":
        extrows = run_remote_query(dbvendor, f"""
                SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p,
                q, r, s, t, u, v, w, x, y, z, aa, bb, cc, dd, ee,
                ff, gg, hh, ii, jj, kk, ll, mm FROM mytable
                """)
    elif dbvendor == "postgres":
        extrows = run_remote_query(dbvendor, f"""
                SELECT col_smallint, col_integer, col_bigint, col_decimal, col_numeric, col_real,
                col_double, col_smallserial, col_serial, col_bigserial, col_money, col_char, 
                col_varchar, col_text, col_bytea, col_boolean,
                col_date, col_time, col_timetz, col_timestamp, col_timestamptz, col_interval,
                col_uuid, col_json, col_jsonb, col_xml, col_inet, col_cidr, col_macaddr,
                col_macaddr8, col_tsvector, col_tsquery, col_bit, col_varbit, col_int4range,
                col_int8range, col_numrange, col_daterange, col_tsrange, col_tstzrange, col_int_array,
                col_text_array, col_jsonb_array, col_point, col_identity, col_not_null, col_unique, 
                col_primary_key 
                FROM mytable
                """)
    else:
        extrows = run_remote_query(dbvendor, f"""
                SELECT id, binary_double_col, binary_float_col, float_col, number_col,
                long_col, date_col, interval_ds_col, interval_ym_col, timestamp_col,
                timestamp_tz_col, timestamp_ltz_col, char_col, nchar_col,
                nvarchar2_col, varchar_col, varchar2_col, raw_col,
                bfile_col, blob_col, clob_col, nclob_col, rowid_col, urowid_col 
                FROM mytable
                """)
    assert len(extrows) > 0
    assert len(rows) > 0
    assert len(rows) == len(extrows)
    if dbvendor == "mysql":
        for row, extrow in zip(rows, extrows):
            assert row[0] == Decimal(extrow[0])
            assert row[1] == Decimal(extrow[1])
            assert row[2] == extrow[2]
            assert row[3] == Decimal(extrow[3])
            assert row[4] == Decimal(extrow[4])
            assert int(row[5],2) == int.from_bytes(extrow[5].encode("latin1"), byteorder='big')
            assert row[6] == float(extrow[6])
            assert row[7] == float(extrow[7])
            assert row[8] == float(extrow[8])
            assert row[9] == float(extrow[9])
            assert row[10] == float(extrow[10])
            assert row[11] == float(extrow[11])
            assert row[12] == float(extrow[12])
            assert round(row[13],1) == round(float(extrow[13]),1)
            assert round(row[14],1) == round(float(extrow[14]),1)
            assert row[15] == int(extrow[15])
            assert row[16] == int(extrow[16])
            assert row[17] == int(extrow[17])
            assert row[18] == int(extrow[18])
            assert row[19] == int(extrow[19])
            assert row[20] == int(extrow[20])
            assert row[21] == int(extrow[21])
            assert row[22] == int(extrow[22])
            assert row[23] == int(extrow[23])
            assert row[24] == datetime.strptime(extrow[24], "%Y-%m-%d").date()
            assert row[25] == datetime.strptime(extrow[25], "%Y-%m-%d %H:%M:%S")
            assert row[26] == datetime.strptime(extrow[26], "%H:%M:%S").time()
            assert row[27].astimezone(timezone.utc) == datetime.strptime(extrow[27], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            assert row[28].tobytes().hex().upper() == extrow[28]
            assert row[29].tobytes().hex().upper() == extrow[29]
            assert row[30].tobytes().decode('utf-8') == extrow[30]
            assert row[31].tobytes().decode('utf-8') == extrow[31]
            assert row[32].tobytes().decode('utf-8') == extrow[32]
            assert row[33] == extrow[33]
            assert row[34] == extrow[34]
            assert row[35] == extrow[35]
            assert row[36] == extrow[36]
    elif dbvendor == "sqlserver":
        for row, extrow in zip(rows, extrows):
            assert row[0] == int(extrow[0])
            assert row[1] == Decimal(extrow[1])
            assert row[2] == Decimal(extrow[2])
            assert row[3] == Decimal(extrow[3])
            assert row[4] == Decimal(extrow[4])
            assert row[5] == int(extrow[5])
            assert round(row[6],3) == round(float(extrow[6]), 3)
            assert row[7] == float(extrow[7])
            assert row[8] == int(extrow[8])
            assert row[9] == int(extrow[9])
            assert row[10] == int(extrow[10])
            assert row[11] == int(extrow[11])
            assert row[12] == Decimal(extrow[12])
            assert Decimal(row[13].replace('$', '').replace(',', '')) == Decimal(extrow[13])
            assert Decimal(row[14].replace('$', '').replace(',', '')) == Decimal(extrow[14])
            assert row[15] == datetime.strptime(extrow[15], "%Y-%m-%d").date()
            assert row[16] == parse_time_with_fraction(extrow[16])
            assert row[17] == parse_datetime_with_fraction(extrow[17])
            assert abs(row[18] -  parse_datetime_with_fraction_and_tz(extrow[18])).total_seconds() < 0.00001
            assert row[19] == parse_datetime_with_fraction(extrow[19])
            assert row[20] == datetime.strptime(extrow[20], "%Y-%m-%d %H:%M:%S")
            assert row[21] == extrow[21]
            assert row[22] == extrow[22]
            assert row[23] == extrow[23]
            assert row[24] == extrow[24]
            assert row[25] == extrow[25]
            assert row[26] == extrow[26]
            assert row[27].tobytes().ljust(10, b'\x00') == bytes.fromhex(extrow[27].removeprefix("0x"))
            assert row[28].tobytes() == bytes.fromhex(extrow[28].removeprefix("0x"))
            assert row[29].tobytes() == bytes.fromhex(extrow[29].removeprefix("0x"))
            assert row[30] == 'AAAAAAEMAAAAAAAA8D8AAAAAAADwPw==' #geo
            assert row[31] == '5hAAAAEMAAAAAAAA8D8AAAAAAADwPw==' #geo
            assert row[32] == extrow[32]
            assert row[32] == extrow[32]
            assert row[33] == extrow[33]
            assert row[34].tobytes() == bytes.fromhex(extrow[34].removeprefix("0x"))
            assert row[35].lower() == extrow[35].lower()
            assert abs((datetime.combine(datetime.today(), row[36]) - datetime.combine(datetime.today(), parse_time_with_fraction(extrow[36]))).total_seconds()) < 0.0005

            #assert row[36] == parse_time_with_fraction(extrow[36])
            assert row[37] == parse_datetime_with_fraction(extrow[37])
            assert row[38] == parse_datetime_with_fraction_and_tz(extrow[38])
    elif dbvendor == "postgres":
        for row, extrow in zip(rows, extrows):
            assert row[0] == int(extrow[0])
            assert row[1] == int(extrow[1])
            assert row[2] == int(extrow[2])
            assert row[3] == Decimal(extrow[3])
            assert row[4] == Decimal(extrow[4])
            assert Decimal(str(row[5])) == Decimal(extrow[5])
            assert Decimal(str(row[6])) == Decimal(extrow[6])
            assert row[7] == int(extrow[7])
            assert row[8] == int(extrow[8])
            assert row[9] == int(extrow[9])
            assert row[10] == extrow[10]
            assert row[11] == extrow[11]
            assert row[12] == extrow[12]
            assert row[13] == extrow[13]
            assert row[14].tobytes() == bytes.fromhex(extrow[14][2:])
            assert row[15] == True and extrow[15] == "t"
            assert row[16] == datetime.strptime(extrow[16], "%Y-%m-%d").date()
            assert row[17] == datetime.strptime(extrow[17], "%H:%M:%S").time()
            assert row[18] == datetime.strptime(extrow[18].replace("+00", "+0000").replace("-00", "-0000"), "%H:%M:%S%z").timetz()
            assert row[19] == datetime.strptime(extrow[19], "%Y-%m-%d %H:%M:%S")
            #assert row[20].astimezone(datetime.timezone.utc) == datetime.datetime.fromisoformat(extrow[20])
            assert row[21] == parse_timedelta(extrow[21])
            assert row[22] == extrow[22]
            assert row[23] == json.loads(extrow[23])
            assert row[24] == json.loads(extrow[24])
            assert row[25] == extrow[25]
            assert row[26] == extrow[26]
            assert row[27] == extrow[27]
            assert row[28] == extrow[28]
            assert row[29] == extrow[29]
            assert row[30] == None
            assert row[31] == None
            assert row[32] == extrow[32]
            assert row[33] == extrow[33]
            assert row[34] == NumericRange(*map(int, extrow[34][1:-1].split(',')), bounds=extrow[34][0] + extrow[34][-1])
            assert row[35] == NumericRange(*map(int, extrow[35][1:-1].split(',')), bounds=extrow[35][0] + extrow[35][-1])
            assert row[36] == NumericRange(*map(Decimal, extrow[36][1:-1].split(',')), bounds=extrow[36][0] + extrow[36][-1])
            assert row[37] == DateRange(date.fromisoformat(extrow[37][1:-1].split(',',1)[0]), date.fromisoformat(extrow[37][1:-1].split(',',1)[1]),bounds=extrow[37][0] + extrow[37][-1])
            assert row[38] == DateTimeRange(datetime.fromisoformat(extrow[38][2:-2].split('","',1)[0]), datetime.fromisoformat(extrow[38][2:-2].split('","',1)[1]), bounds=extrow[38][0] + extrow[38][-1])
            #assert row[39] == DateTimeTZRange(*(datetime.fromisoformat(x).astimezone(timezone.utc) for x in extrow[39][2:-2].split('","')), extrow[39][0] + extrow[39][-1])
            assert row[39] == DateTimeTZRange(*(datetime.fromisoformat((x + ':00') if (len(x) >= 3 and x[-3] in '+-' and x[-2:].isdigit()) else (x[:-2] + ':' + x[-2:] if (len(x) >= 5 and x[-5] in '+-' and x[-4:].isdigit()) else x)).astimezone(timezone.utc) for x in extrow[39][2:-2].split('","')), extrow[39][0] + extrow[39][-1])

            assert row[40] == list(map(int, extrow[40].strip("{}").split(",")))
            assert row[41] == list(map(str, extrow[41].strip("{}").split(",")))
            assert row[42] == [json.loads(json.loads(x)) for x in extrow[42][1:-1].split(',')]
            assert row[43] == extrow[43]
            assert row[44] == int(extrow[44])
            assert row[45] == extrow[45]
            assert row[46] == extrow[46]
            assert row[47] == int(extrow[47])
    else:
        for row, extrow in zip(rows, extrows):
            assert row[0] == int(extrow[0])
            assert round(row[1], -1) == float(extrow[1])
            assert round(row[2], 0) == float(extrow[2])
            assert row[3] == Decimal(extrow[3])
            assert row[4] == Decimal(extrow[4])
            if dbvendor != "olr":   #olr does not support long text
                assert row[5] == extrow[5]
            assert row[6].date() == datetime.strptime(extrow[6], "%d-%b-%y").date()
            assert row[7] == parse_ora_day2second_interval(extrow[7])
            assert row[8] == parse_ora_year2month_interval(extrow[8])
            assert row[9] == datetime.strptime(extrow[9], "%d-%b-%y %H.%M.%S.%f %p")
            assert row[10].astimezone(timezone.utc) == datetime.strptime(extrow[10], "%d-%b-%y %H.%M.%S.%f %p %z").astimezone(timezone.utc)
            # skip this check as it depends on oracle instance's timezone
            #assert row[11].astimezone(timezone.utc) == datetime.strptime(extrow[11], "%d-%b-%y %H.%M.%S.%f %p").replace(tzinfo=row[11].tzinfo).astimezone(timezone.utc)
            assert row[12].rstrip() == extrow[12]
            assert row[13].rstrip() == extrow[13]
            assert row[14] == extrow[14]
            assert row[15] == extrow[15]
            assert row[16] == extrow[16]
            assert row[17].tobytes().hex().upper() == extrow[17] or row[17].tobytes().hex().upper() == extrow[17].lower().encode().hex()
            assert row[18] == None
            assert row[19].tobytes().hex().upper() == extrow[19] or row[19].tobytes().hex().upper() == extrow[19].lower().encode().hex()
            assert row[20] == extrow[20]
            assert row[21] == extrow[21]
            assert row[22] == None
            assert row[23] == None
    
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    run_remote_query(dbvendor, "DROP TABLE mytable")
    time.sleep(5)

def test_TableNameMapping(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap_tnm"
    dbname = getDbname(dbvendor)
    
    if dbvendor == "mysql":
        exttable_prefix=dbname
    else:
        schema = getSchema(dbvendor)
        exttable_prefix= dbname + "." + schema
        
    # create objmap of type = 'table'
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.OBJMAP_SRCTABLE1', '{dbname.lower()}.objmap_dsttable1')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.OBJMAP_SRCTABLE2', 'objmap_dsttable2')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.OBJMAP_SRCTABLE3', 'someschema.objmap_dsttable3')")
        assert rows[0] == 0

    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.objmap_srctable1', '{dbname.lower()}.objmap_dsttable1')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.objmap_srctable2', 'objmap_dsttable2')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.objmap_srctable3', 'someschema.objmap_dsttable3')")
        assert rows[0] == 0

    if dbvendor == "postgres":
        # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

        # we need to start connector now so tables are copied via CDC, debezium does not snapshot table schemas
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
        assert result == 0
        time.sleep(20)

    # create the tables remotely
    run_remote_query(dbvendor, "CREATE TABLE objmap_srctable1 (a INT, b varchar(50))")
    run_remote_query(dbvendor, "CREATE TABLE objmap_srctable2 (a INT, b varchar(50))")
    run_remote_query(dbvendor, "CREATE TABLE objmap_srctable3 (a INT, b varchar(50))")
    
    # special case for sqlserver: add new tables to cdc
    if dbvendor == "sqlserver":
        run_remote_query(dbvendor, f"""
            EXEC sys.sp_cdc_enable_table @source_schema = '{schema}',
            @source_name = 'objmap_srctable1', @role_name = NULL, @supports_net_changes = 0;
            """)
        run_remote_query(dbvendor, f"""
            EXEC sys.sp_cdc_enable_table @source_schema = '{schema}',
            @source_name = 'objmap_srctable2', @role_name = NULL, @supports_net_changes = 0;
            """)
        run_remote_query(dbvendor, f"""
            EXEC sys.sp_cdc_enable_table @source_schema = '{schema}',
            @source_name = 'objmap_srctable3', @role_name = NULL, @supports_net_changes = 0;
            """)

    # create the connector in pg and copy the tables
    if dbvendor != "postgres":
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
        assert result == 0
    
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    # check if tables have been copied with table names mapped correctly
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.OBJMAP_SRCTABLE1' LIMIT 1")
        assert rows != None and len(rows) > 0 and rows[0] == f'{dbname.lower()}.objmap_dsttable1'
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.OBJMAP_SRCTABLE2' LIMIT 1")
        assert rows != None and len(rows) > 0 and rows[0] == f'public.objmap_dsttable2'
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.OBJMAP_SRCTABLE3' LIMIT 1")
        assert rows != None and len(rows) > 0 and rows[0] == f'someschema.objmap_dsttable3'
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.objmap_srctable1' LIMIT 1")
        assert rows != None and len(rows) > 0 and rows[0] == f'{dbname.lower()}.objmap_dsttable1'
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.objmap_srctable2' LIMIT 1")
        assert rows != None and len(rows) > 0 and rows[0] == f'public.objmap_dsttable2'
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.objmap_srctable3' LIMIT 1")
        assert rows != None and len(rows) > 0 and rows[0] == f'someschema.objmap_dsttable3'

    # make sure they exist
    rows = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = '{dbname.lower()}' AND table_name ='objmap_dsttable1')")
    assert rows[0] == True
    rows = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name ='objmap_dsttable2')")
    assert rows[0] == True
    rows = run_pg_query_one(pg_cursor, f"SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = 'someschema' AND table_name ='objmap_dsttable3')")
    assert rows[0] == True
    
    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

    run_remote_query(dbvendor, "DROP TABLE objmap_srctable1")
    run_remote_query(dbvendor, "DROP TABLE objmap_srctable2")
    run_remote_query(dbvendor, "DROP TABLE objmap_srctable3")
    time.sleep(5)

def test_ColumnNameMapping(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap_cnm"
    dbname = getDbname(dbvendor)

    if dbvendor == "mysql":
        exttable_prefix=dbname
    else:
        schema = getSchema(dbvendor)
        exttable_prefix= dbname + "." + schema

    # create objmap of type = 'column'
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.OBJMAPCOL_SRCTABLE1.A', 'pgintcol')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.OBJMAPCOL_SRCTABLE1.B', 'pgtextcol')")
        assert rows[0] == 0
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.objmapcol_srctable1.a', 'pgintcol')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.objmapcol_srctable1.b', 'pgtextcol')")
        assert rows[0] == 0

    if dbvendor == "postgres":
	    # postgres in debezium snapshot needs to create tables manually
        run_pg_query_one(pg_cursor, f"CREATE SCHEMA IF NOT EXISTS {dbname}")
        run_pg_query_one(pg_cursor, f"CREATE TABLE {dbname}.orders (order_number int primary key, order_date timestamp without time zone, purchaser int, quantity int , product_id int)")

        # we need to start connector now so tables are copied via CDC, debezium does not snapshot table schemas
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
        assert result == 0
        time.sleep(20)

    # create the tables remotely
    run_remote_query(dbvendor, "CREATE TABLE objmapcol_srctable1 (a INT, b varchar(50))")

    # special case for sqlserver: add new tables to cdc
    if dbvendor == "sqlserver":
        run_remote_query(dbvendor, f"""
            EXEC sys.sp_cdc_enable_table @source_schema = '{schema}',
            @source_name = 'objmapcol_srctable1', @role_name = NULL, @supports_net_changes = 0;
            """)

    if dbvendor != "postgres":
        # create the connector inpg and copy the tables
        result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
        assert result == 0
    
    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    # check if tables have been copied with table names mapped correctly
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.OBJMAPCOL_SRCTABLE1'")
        assert rows != None and rows[0][1] == 'pgintcol'
        assert rows != None and rows[1][1] == 'pgtextcol'
    else:
        rows = run_pg_query(pg_cursor, f"SELECT ext_attname, pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.objmapcol_srctable1'")
        assert rows != None and rows[0][1] == 'pgintcol'
        assert rows != None and rows[1][1] == 'pgtextcol'

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")

    run_remote_query(dbvendor, "DROP TABLE objmapcol_srctable1")
    time.sleep(5)

def test_DataTypeMapping(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap_dtm"
    dbname = getDbname(dbvendor)

    if dbvendor == "postgres":
        # for postgres connector, we need to use fdw snapshot to observe
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    if dbvendor == "mysql":
        exttable_prefix=dbname
    else:
        schema = getSchema(dbvendor)
        exttable_prefix= dbname + "." + schema

    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.ORDERS.ORDER_DATE', 'text|0')")
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.orders.order_date', 'text|0')")
    assert rows[0] == 0

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    # orders table shall have been replicated
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'ORDER_DATE'")
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'order_date'")
    assert rows[0] == f"text"

    rows = run_pg_query_one(pg_cursor, f"SELECT order_date from {dbname}.orders")
    assert isinstance(rows[0], str)

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    if dbvendor == "postgres":
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)

def test_TransformExpression(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap_te"
    dbname = getDbname(dbvendor)

    if dbvendor == "postgres":
        # for postgres connector, we need to use fdw snapshot to observe
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)
    
    if dbvendor == "mysql":
        exttable_prefix=dbname
    else:
        schema = getSchema(dbvendor)
        exttable_prefix= dbname + "." + schema

    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'transform', '{exttable_prefix}.ORDERS.PURCHASER', '%d + 1000000')")
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'transform', '{exttable_prefix}.orders.purchaser', '%d + 1000000')")
    assert rows[0] == 0

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    # orders table shall have been replicated
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'PURCHASER'")
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'purchaser'")
    assert rows[0] == f"%d + 1000000"

    rows = run_pg_query(pg_cursor, f"SELECT purchaser from {dbname}.orders")
    assert len(rows) > 0
    for row in rows:
        assert row[0] > 1000000

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    if dbvendor == "postgres":
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)

def test_ReloadObjmapEntries(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap_roe"
    dbname = getDbname(dbvendor)

    if dbvendor == "postgres":
        # for postgres connector, we need to use fdw snapshot to observe
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'fdw'", True)

    if dbvendor == "mysql":
        exttable_prefix=dbname
    else:
        schema = getSchema(dbvendor)
        exttable_prefix= dbname + "." + schema

    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial")
    assert result == 0

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)

    # default table orders table shall have been replicated
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' LIMIT 1")
        assert rows[0] == f"{dbname.lower()}.orders"

        # column names shall be defaults too
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='ORDER_NUMBER'")
        assert rows[0] == f"order_number"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'ORDER_DATE'")
        assert rows[0] == f"order_date"

        # transform expression shall be empty
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='ORDER_NUMBER'")
        assert rows[0] == None
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='ORDER_DATE'")
        assert rows[0] == None

        # data type as default
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='ORDER_NUMBER'")
        assert rows[0] == "integer" or rows[0] == "numeric"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='ORDER_DATE'")
        assert rows[0] == "date" or rows[0] == "timestamp without time zone"

        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.ORDERS', '{dbname.lower()}.invoices')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.ORDERS.ORDER_NUMBER', 'the_number')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.ORDERS.ORDER_DATE', 'the_date')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.ORDERS.PURCHASER', 'bigint')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.ORDERS.QUANTITY', 'text|0')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.ORDERS.ORDER_NUMBER', 'bigint')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.ORDERS.ORDER_DATE', 'text|0')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'transform', '{exttable_prefix}.ORDERS.ORDER_NUMBER', '%d + 1000000')")
        assert rows[0] == 0
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' LIMIT 1")
        assert rows[0] == f"{dbname.lower()}.orders"

        # column names shall be defaults too
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='order_number'")
        assert rows[0] == f"order_number"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'order_date'")
        assert rows[0] == f"order_date"

        # transform expression shall be empty
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='order_number'")
        assert rows[0] == None
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='order_date'")
        assert rows[0] == None

        # data type as default
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='order_number'")
        assert rows[0] == "integer" or rows[0] == "numeric"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='order_date'")
        assert rows[0] == "date" or rows[0] == "timestamp without time zone"

        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', '{exttable_prefix}.orders', '{dbname.lower()}.invoices')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.orders.order_number', 'the_number')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', '{exttable_prefix}.orders.order_date', 'the_date')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.orders.purchaser', 'bigint')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.orders.quantity', 'text|0')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.orders.order_number', 'bigint')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', '{exttable_prefix}.orders.order_date', 'text|0')")
        assert rows[0] == 0
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'transform', '{exttable_prefix}.orders.order_number', '%d + 1000000')")
        assert rows[0] == 0

    # reload connector
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_reload_objmap('{name}')")
    assert rows[0] == 0

    time.sleep(40)
    if dbvendor == "oracle" or dbvendor == "olr":
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' LIMIT 1")
        assert rows[0] == f"{dbname.lower()}.invoices"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='ORDER_NUMBER'")
        assert rows[0] == f"the_number"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'ORDER_DATE'")
        assert rows[0] == f"the_date"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname ='PURCHASER'")
        assert rows[0] == f"bigint"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'QUANTITY'")
        assert rows[0] == f"text"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'ORDER_NUMBER'")
        assert rows[0] == f"bigint"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'ORDER_DATE'")
        assert rows[0] == f"text"
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.ORDERS' AND ext_attname = 'ORDER_NUMBER'")
        assert rows[0] == f"%d + 1000000"
    else:
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_tbname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' LIMIT 1")
        assert rows[0] == f"{dbname.lower()}.invoices"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='order_number'")
        assert rows[0] == f"the_number"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_attname FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'order_date'")
        assert rows[0] == f"the_date"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname ='purchaser'")
        assert rows[0] == f"bigint"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'quantity'")
        assert rows[0] == f"text"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'order_number'")
        assert rows[0] == f"bigint"
        rows = run_pg_query_one(pg_cursor, f"SELECT pg_atttypename FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'order_date'")
        assert rows[0] == f"text"
        rows = run_pg_query_one(pg_cursor, f"SELECT transform FROM synchdb_att_view WHERE name = '{name}' AND ext_tbname = '{exttable_prefix}.orders' AND ext_attname = 'order_number'")
        assert rows[0] == f"%d + 1000000"

    rows = run_pg_query(pg_cursor, f"SELECT the_number, the_date, quantity FROM {dbname.lower()}.invoices")
    assert len(rows) > 0
    for row in rows:
        assert row[0] < 1000000
        assert isinstance(row[1], str)
        assert isinstance(row[2], str)
    if dbvendor == "msql":
        extrows = run_remote_query(dbvendor, f"""
            INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES
                (10005, "2025-12-12", 1002, 10000, 102)
            """)
    elif dbvendor == "oracle" or dbvendor == "olr":
        extrows = run_remote_query(dbvendor, f"""
            INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES
                (10005, TO_DATE('2025-12-12', 'YYYY-MM-DD'), 1002, 10000, 102)
            """)
    elif dbvendor == "postgres":
        extrows = run_remote_query(dbvendor, f"""
            INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES
                (10005, '2025-12-12', 1002, 10000, 102)
            """)
    else:
        extrows = run_remote_query(dbvendor, f"""
            INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES
                ("2025-12-12", 1002, 10000, 102)
            """)

    if dbvendor == "oracle" or dbvendor == "olr":
        time.sleep(60)
    else:
        time.sleep(20)
        
    rows = run_pg_query(pg_cursor, f"SELECT the_number, the_date, quantity FROM {dbname.lower()}.invoices WHERE the_number > 1000000")
    assert len(rows) > 0
    for row in rows:
        assert row[0] > 1000000
        assert isinstance(row[1], str)
        assert isinstance(row[2], str)

    stop_and_delete_synchdb_connector(pg_cursor, name)
    drop_default_pg_schema(pg_cursor, dbvendor)
    drop_repslot_and_pub(dbvendor, name, "postgres")
    if dbvendor == "postgres":
        update_guc_conf(pg_cursor, "synchdb.snapshot_engine", "'debezium'", True)

    run_remote_query(dbvendor, f"DELETE FROM orders WHERE order_number>=10005")
    time.sleep(5)
    
def test_TransformExpressionWithError(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Numeric(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Datetime(pg_cursor, dbvendor):
    #not yet supported
    assert True

def test_ConvertString2Date(pg_cursor, dbvendor):
    #not yet supported
    assert True

def test_ConvertString2Time(pg_cursor, dbvendor):
    #not yet supported
    assert True

def test_ConvertString2Bit(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Timestamp(pg_cursor, dbvendor):
    #not yet supported
    assert True

def test_ConvertString2Binary(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Interval(pg_cursor, dbvendor):
    #not yet supported
    assert True


