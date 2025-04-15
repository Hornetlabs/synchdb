import common
import time
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from binascii import unhexlify

from common import run_pg_query, run_pg_query_one, run_remote_query, run_remote_query_one, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, create_and_start_synchdb_connector, stop_and_delete_synchdb_connector

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

def test_AllDefaultDataTypes(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_addt"
    dbname = getDbname(dbvendor).lower()
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
    if dbvendor == "oracle":
        time.sleep(30)
    else:
        time.sleep(10)
    
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
    if dbvendor == "oracle":
        time.sleep(60)
    else:
        time.sleep(5)

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
    else:
        extrows = run_remote_query(dbvendor, f"""
                SELECT id, binary_double_col, binary_float_col, float_col, number_col,
                long_col, date_col, interval_ds_col, interval_ym_col, timestamp_col,
                timestamp_tz_col, timestamp_ltz_col, char_col, nchar_col,
                nvarchar2_col, varchar_col, varchar2_col, raw_col,
                bfile_col, blob_col, clob_col, nclob_col, rowid_col, urowid_col 
                FROM mytable
                """)
    assert len(rows) > 0
    assert len(extrows) > 0
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
    else:
        for row, extrow in zip(rows, extrows):
            assert row[0] == int(extrow[0])
            assert round(row[1], -1) == float(extrow[1])
            assert round(row[2], 0) == float(extrow[2])
            assert row[3] == Decimal(extrow[3])
            assert row[4] == Decimal(extrow[4])
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
            assert row[17].tobytes().hex().upper() == extrow[17]
            assert row[18] == None
            assert row[19].tobytes().hex().upper() == extrow[19]
            assert row[20] == extrow[20]
            assert row[21] == extrow[21]
            assert row[22] == None
            assert row[23] == None

    stop_and_delete_synchdb_connector(pg_cursor, name)

def test_CreateObjmapEntries(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap"
    dbname = getDbname(dbvendor).lower()
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'table', 'ext_db1.ext_table1', 'pg_table1')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'column', 'ext_db1.ext_table1.ext_column1', 'pg_column1')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', 'int', 'bigint')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'datatype', 'ext_db1.ext_table1.ext_column1', 'text')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_add_objmap('{name}', 'transform', 'ext_db1.ext_table1.ext_column1', '''>>>>>'' || ''%d'' || ''<<<<<''')")
    assert rows[0] == 0

    rows = run_pg_query_one(pg_cursor, f"SELECT count(*) from synchdb_objmap WHERE name = '{name}'")
    assert rows[0] == 5

    rows = run_pg_query(pg_cursor, f"SELECT enabled from synchdb_objmap WHERE name = '{name}'")
    for row in rows:
        assert row[0] == True

def test_CreateObjmapEntriesWithError(pg_cursor, dbvendor):
    assert True

def test_DeleteObjmapEntries(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_objmap"
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_objmap('{name}', 'table', 'ext_db1.ext_table1')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_objmap('{name}', 'column', 'ext_db1.ext_table1.ext_column1')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_objmap('{name}', 'datatype', 'int')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_objmap('{name}', 'datatype', 'ext_db1.ext_table1.ext_column1')")
    assert rows[0] == 0
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_del_objmap('{name}', 'transform', 'ext_db1.ext_table1.ext_column1')")
    assert rows[0] == 0

    rows = run_pg_query_one(pg_cursor, f"SELECT count(*) from synchdb_objmap WHERE name = '{name}'")
    assert rows[0] == 5

    rows = run_pg_query(pg_cursor, f"SELECT enabled from synchdb_objmap WHERE name = '{name}'")
    for row in rows:
        assert row[0] == False

def test_ReloadObjmapEntries(pg_cursor, dbvendor):
    assert True

def test_TableNameMapping(pg_cursor, dbvendor):
    assert True

def test_AlterTableNameMapping(pg_cursor, dbvendor):
    assert True

def test_ColumnNameMapping(pg_cursor, dbvendor):
    assert True

def test_AlterColumnNameMapping(pg_cursor, dbvendor):
    assert True

def test_DataTypeMapping(pg_cursor, dbvendor):
    assert True

def test_AlterDataTypeMapping(pg_cursor, dbvendor):
    assert True

def test_DataTypeMappingWithError(pg_cursor, dbvendor):
    assert True

def test_TransformExpression(pg_cursor, dbvendor):
    assert True

def test_AlterTransformExpression(pg_cursor, dbvendor):
    assert True

def test_TransformExpressionWithError(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Numeric(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Datetime(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Date(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Time(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Bit(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Timestamp(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Binary(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Interval(pg_cursor, dbvendor):
    assert True


