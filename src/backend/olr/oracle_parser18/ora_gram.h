/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_ORA_BASE_YY_ORA_GRAM_H_INCLUDED
# define YY_ORA_BASE_YY_ORA_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int ora_base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENT = 258,                   /* IDENT  */
    UIDENT = 259,                  /* UIDENT  */
    FCONST = 260,                  /* FCONST  */
    SCONST = 261,                  /* SCONST  */
    USCONST = 262,                 /* USCONST  */
    BCONST = 263,                  /* BCONST  */
    XCONST = 264,                  /* XCONST  */
    Op = 265,                      /* Op  */
    BFCONST = 266,                 /* BFCONST  */
    BDCONST = 267,                 /* BDCONST  */
    ICONST = 268,                  /* ICONST  */
    PARAM = 269,                   /* PARAM  */
    ORAPARAM = 270,                /* ORAPARAM  */
    TYPECAST = 271,                /* TYPECAST  */
    DOT_DOT = 272,                 /* DOT_DOT  */
    COLON_EQUALS = 273,            /* COLON_EQUALS  */
    EQUALS_GREATER = 274,          /* EQUALS_GREATER  */
    LESS_EQUALS = 275,             /* LESS_EQUALS  */
    GREATER_EQUALS = 276,          /* GREATER_EQUALS  */
    NOT_EQUALS = 277,              /* NOT_EQUALS  */
    LESS_LESS = 278,               /* LESS_LESS  */
    GREATER_GREATER = 279,         /* GREATER_GREATER  */
    ABORT_P = 280,                 /* ABORT_P  */
    ABSENT = 281,                  /* ABSENT  */
    ABSOLUTE_P = 282,              /* ABSOLUTE_P  */
    ACCESS = 283,                  /* ACCESS  */
    ACTION = 284,                  /* ACTION  */
    ADD_P = 285,                   /* ADD_P  */
    ADMIN = 286,                   /* ADMIN  */
    AFTER = 287,                   /* AFTER  */
    AGGREGATE = 288,               /* AGGREGATE  */
    ALL = 289,                     /* ALL  */
    ALSO = 290,                    /* ALSO  */
    ALTER = 291,                   /* ALTER  */
    ALWAYS = 292,                  /* ALWAYS  */
    ANALYSE = 293,                 /* ANALYSE  */
    ANALYZE = 294,                 /* ANALYZE  */
    AND = 295,                     /* AND  */
    ANY = 296,                     /* ANY  */
    ARRAY = 297,                   /* ARRAY  */
    AS = 298,                      /* AS  */
    ASC = 299,                     /* ASC  */
    ASENSITIVE = 300,              /* ASENSITIVE  */
    ASSERTION = 301,               /* ASSERTION  */
    ASSIGNMENT = 302,              /* ASSIGNMENT  */
    ASYMMETRIC = 303,              /* ASYMMETRIC  */
    ATOMIC = 304,                  /* ATOMIC  */
    AT = 305,                      /* AT  */
    ATTACH = 306,                  /* ATTACH  */
    ATTRIBUTE = 307,               /* ATTRIBUTE  */
    AUTHORIZATION = 308,           /* AUTHORIZATION  */
    BACKWARD = 309,                /* BACKWARD  */
    BEFORE = 310,                  /* BEFORE  */
    BEGIN_P = 311,                 /* BEGIN_P  */
    BETWEEN = 312,                 /* BETWEEN  */
    BIGINT = 313,                  /* BIGINT  */
    BINARY = 314,                  /* BINARY  */
    BINARY_DOUBLE = 315,           /* BINARY_DOUBLE  */
    BINARY_FLOAT = 316,            /* BINARY_FLOAT  */
    BIT = 317,                     /* BIT  */
    BODY = 318,                    /* BODY  */
    BOOLEAN_P = 319,               /* BOOLEAN_P  */
    BOTH = 320,                    /* BOTH  */
    BREADTH = 321,                 /* BREADTH  */
    BY = 322,                      /* BY  */
    BYTE_P = 323,                  /* BYTE_P  */
    CACHE = 324,                   /* CACHE  */
    CALL = 325,                    /* CALL  */
    CALLED = 326,                  /* CALLED  */
    CASCADE = 327,                 /* CASCADE  */
    CASCADED = 328,                /* CASCADED  */
    CASE = 329,                    /* CASE  */
    CAST = 330,                    /* CAST  */
    CATALOG_P = 331,               /* CATALOG_P  */
    CHAIN = 332,                   /* CHAIN  */
    CHAR_P = 333,                  /* CHAR_P  */
    CHARACTER = 334,               /* CHARACTER  */
    CHARACTERISTICS = 335,         /* CHARACTERISTICS  */
    CHECK = 336,                   /* CHECK  */
    CHECKPOINT = 337,              /* CHECKPOINT  */
    CLASS = 338,                   /* CLASS  */
    CLOSE = 339,                   /* CLOSE  */
    CLUSTER = 340,                 /* CLUSTER  */
    COALESCE = 341,                /* COALESCE  */
    COLLATE = 342,                 /* COLLATE  */
    COLLATION = 343,               /* COLLATION  */
    COLUMN = 344,                  /* COLUMN  */
    COLUMNS = 345,                 /* COLUMNS  */
    COMMENT = 346,                 /* COMMENT  */
    COMMENTS = 347,                /* COMMENTS  */
    COMMIT = 348,                  /* COMMIT  */
    COMMITTED = 349,               /* COMMITTED  */
    COMPRESSION = 350,             /* COMPRESSION  */
    CONCURRENTLY = 351,            /* CONCURRENTLY  */
    CONDITIONAL = 352,             /* CONDITIONAL  */
    CONFIGURATION = 353,           /* CONFIGURATION  */
    CONFLICT = 354,                /* CONFLICT  */
    CONNECTION = 355,              /* CONNECTION  */
    CONSTRAINT = 356,              /* CONSTRAINT  */
    CONSTRAINTS = 357,             /* CONSTRAINTS  */
    CONTENT_P = 358,               /* CONTENT_P  */
    CONTINUE_P = 359,              /* CONTINUE_P  */
    CONVERSION_P = 360,            /* CONVERSION_P  */
    COPY = 361,                    /* COPY  */
    COST = 362,                    /* COST  */
    CREATE = 363,                  /* CREATE  */
    CROSS = 364,                   /* CROSS  */
    CSV = 365,                     /* CSV  */
    CUBE = 366,                    /* CUBE  */
    CURRENT_P = 367,               /* CURRENT_P  */
    CURRENT_CATALOG = 368,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 369,            /* CURRENT_DATE  */
    CURRENT_ROLE = 370,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 371,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 372,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 373,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 374,            /* CURRENT_USER  */
    CURSOR = 375,                  /* CURSOR  */
    CYCLE = 376,                   /* CYCLE  */
    DATA_P = 377,                  /* DATA_P  */
    DATABASE = 378,                /* DATABASE  */
    DATE_P = 379,                  /* DATE_P  */
    DAY_P = 380,                   /* DAY_P  */
    DEALLOCATE = 381,              /* DEALLOCATE  */
    DEC = 382,                     /* DEC  */
    DECIMAL_P = 383,               /* DECIMAL_P  */
    DECLARE = 384,                 /* DECLARE  */
    DECODE = 385,                  /* DECODE  */
    DEFAULT = 386,                 /* DEFAULT  */
    DEFAULTS = 387,                /* DEFAULTS  */
    DEFERRABLE = 388,              /* DEFERRABLE  */
    DEFERRED = 389,                /* DEFERRED  */
    DEFINER = 390,                 /* DEFINER  */
    DELETE_P = 391,                /* DELETE_P  */
    DELIMITER = 392,               /* DELIMITER  */
    DELIMITERS = 393,              /* DELIMITERS  */
    DEPENDS = 394,                 /* DEPENDS  */
    DEPTH = 395,                   /* DEPTH  */
    DESC = 396,                    /* DESC  */
    DETACH = 397,                  /* DETACH  */
    DICTIONARY = 398,              /* DICTIONARY  */
    DISABLE_P = 399,               /* DISABLE_P  */
    DISCARD = 400,                 /* DISCARD  */
    DISTINCT = 401,                /* DISTINCT  */
    DO = 402,                      /* DO  */
    DOCUMENT_P = 403,              /* DOCUMENT_P  */
    DOMAIN_P = 404,                /* DOMAIN_P  */
    DOUBLE_P = 405,                /* DOUBLE_P  */
    DROP = 406,                    /* DROP  */
    EACH = 407,                    /* EACH  */
    ELSE = 408,                    /* ELSE  */
    EMPTY_P = 409,                 /* EMPTY_P  */
    ENABLE_P = 410,                /* ENABLE_P  */
    ENCODING = 411,                /* ENCODING  */
    ENCRYPTED = 412,               /* ENCRYPTED  */
    END_P = 413,                   /* END_P  */
    ENFORCED = 414,                /* ENFORCED  */
    ENUM_P = 415,                  /* ENUM_P  */
    ERROR_P = 416,                 /* ERROR_P  */
    ESCAPE = 417,                  /* ESCAPE  */
    EVENT = 418,                   /* EVENT  */
    EXCEPT = 419,                  /* EXCEPT  */
    EXCLUDE = 420,                 /* EXCLUDE  */
    EXCLUDING = 421,               /* EXCLUDING  */
    EXCLUSIVE = 422,               /* EXCLUSIVE  */
    EXEC = 423,                    /* EXEC  */
    EXECUTE = 424,                 /* EXECUTE  */
    EXISTS = 425,                  /* EXISTS  */
    EXPLAIN = 426,                 /* EXPLAIN  */
    EXPRESSION = 427,              /* EXPRESSION  */
    EXTEND = 428,                  /* EXTEND  */
    EXTENSION = 429,               /* EXTENSION  */
    EXTERNAL = 430,                /* EXTERNAL  */
    FALSE_P = 431,                 /* FALSE_P  */
    FAMILY = 432,                  /* FAMILY  */
    FETCH = 433,                   /* FETCH  */
    FILTER = 434,                  /* FILTER  */
    FINALIZE = 435,                /* FINALIZE  */
    FIRST_P = 436,                 /* FIRST_P  */
    FLOAT_P = 437,                 /* FLOAT_P  */
    FOLLOWING = 438,               /* FOLLOWING  */
    FOR = 439,                     /* FOR  */
    FORCE = 440,                   /* FORCE  */
    FOREIGN = 441,                 /* FOREIGN  */
    FORMAT = 442,                  /* FORMAT  */
    FORWARD = 443,                 /* FORWARD  */
    FREEZE = 444,                  /* FREEZE  */
    FROM = 445,                    /* FROM  */
    FULL = 446,                    /* FULL  */
    FUNCTION = 447,                /* FUNCTION  */
    FUNCTIONS = 448,               /* FUNCTIONS  */
    GENERATED = 449,               /* GENERATED  */
    GLOBAL = 450,                  /* GLOBAL  */
    GRANT = 451,                   /* GRANT  */
    GRANTED = 452,                 /* GRANTED  */
    GREATEST = 453,                /* GREATEST  */
    GROUP_P = 454,                 /* GROUP_P  */
    GROUPING = 455,                /* GROUPING  */
    GROUPS = 456,                  /* GROUPS  */
    HANDLER = 457,                 /* HANDLER  */
    HAVING = 458,                  /* HAVING  */
    HEADER_P = 459,                /* HEADER_P  */
    HOLD = 460,                    /* HOLD  */
    HOUR_P = 461,                  /* HOUR_P  */
    IDENTITY_P = 462,              /* IDENTITY_P  */
    IF_P = 463,                    /* IF_P  */
    ILIKE = 464,                   /* ILIKE  */
    IMMEDIATE = 465,               /* IMMEDIATE  */
    IMMUTABLE = 466,               /* IMMUTABLE  */
    IMPLICIT_P = 467,              /* IMPLICIT_P  */
    IMPORT_P = 468,                /* IMPORT_P  */
    IN_P = 469,                    /* IN_P  */
    INCLUDE = 470,                 /* INCLUDE  */
    INCLUDING = 471,               /* INCLUDING  */
    INCREMENT = 472,               /* INCREMENT  */
    INDENT = 473,                  /* INDENT  */
    INDEX = 474,                   /* INDEX  */
    INDEXES = 475,                 /* INDEXES  */
    INHERIT = 476,                 /* INHERIT  */
    INHERITS = 477,                /* INHERITS  */
    INITIALLY = 478,               /* INITIALLY  */
    INLINE_P = 479,                /* INLINE_P  */
    INNER_P = 480,                 /* INNER_P  */
    INOUT = 481,                   /* INOUT  */
    INPUT_P = 482,                 /* INPUT_P  */
    INSENSITIVE = 483,             /* INSENSITIVE  */
    INSERT = 484,                  /* INSERT  */
    INSTEAD = 485,                 /* INSTEAD  */
    INT_P = 486,                   /* INT_P  */
    INTEGER = 487,                 /* INTEGER  */
    INTERSECT = 488,               /* INTERSECT  */
    INTERVAL = 489,                /* INTERVAL  */
    INTO = 490,                    /* INTO  */
    INVISIBLE = 491,               /* INVISIBLE  */
    INVOKER = 492,                 /* INVOKER  */
    IS = 493,                      /* IS  */
    ISNULL = 494,                  /* ISNULL  */
    ISOLATION = 495,               /* ISOLATION  */
    JOIN = 496,                    /* JOIN  */
    JSON = 497,                    /* JSON  */
    JSON_ARRAY = 498,              /* JSON_ARRAY  */
    JSON_ARRAYAGG = 499,           /* JSON_ARRAYAGG  */
    JSON_EXISTS = 500,             /* JSON_EXISTS  */
    JSON_OBJECT = 501,             /* JSON_OBJECT  */
    JSON_OBJECTAGG = 502,          /* JSON_OBJECTAGG  */
    JSON_QUERY = 503,              /* JSON_QUERY  */
    JSON_SCALAR = 504,             /* JSON_SCALAR  */
    JSON_SERIALIZE = 505,          /* JSON_SERIALIZE  */
    JSON_TABLE = 506,              /* JSON_TABLE  */
    JSON_VALUE = 507,              /* JSON_VALUE  */
    KEEP = 508,                    /* KEEP  */
    KEY = 509,                     /* KEY  */
    KEYS = 510,                    /* KEYS  */
    LABEL = 511,                   /* LABEL  */
    LANGUAGE = 512,                /* LANGUAGE  */
    LARGE_P = 513,                 /* LARGE_P  */
    LAST_P = 514,                  /* LAST_P  */
    LATERAL_P = 515,               /* LATERAL_P  */
    LEADING = 516,                 /* LEADING  */
    LEAKPROOF = 517,               /* LEAKPROOF  */
    LEAST = 518,                   /* LEAST  */
    LEFT = 519,                    /* LEFT  */
    LEVEL = 520,                   /* LEVEL  */
    LIKE = 521,                    /* LIKE  */
    LIMIT = 522,                   /* LIMIT  */
    LISTEN = 523,                  /* LISTEN  */
    LOAD = 524,                    /* LOAD  */
    LOCAL = 525,                   /* LOCAL  */
    LOCALTIME = 526,               /* LOCALTIME  */
    LOCALTIMESTAMP = 527,          /* LOCALTIMESTAMP  */
    LOCATION = 528,                /* LOCATION  */
    LOCK_P = 529,                  /* LOCK_P  */
    LOCKED = 530,                  /* LOCKED  */
    LOGGED = 531,                  /* LOGGED  */
    MAPPING = 532,                 /* MAPPING  */
    MATCH = 533,                   /* MATCH  */
    MATCHED = 534,                 /* MATCHED  */
    MATERIALIZED = 535,            /* MATERIALIZED  */
    MAXVALUE = 536,                /* MAXVALUE  */
    MERGE = 537,                   /* MERGE  */
    MERGE_ACTION = 538,            /* MERGE_ACTION  */
    METHOD = 539,                  /* METHOD  */
    MINUTE_P = 540,                /* MINUTE_P  */
    MINVALUE = 541,                /* MINVALUE  */
    MODE = 542,                    /* MODE  */
    MODIFY = 543,                  /* MODIFY  */
    MONTH_P = 544,                 /* MONTH_P  */
    MOVE = 545,                    /* MOVE  */
    NAME_P = 546,                  /* NAME_P  */
    NAMES = 547,                   /* NAMES  */
    NATIONAL = 548,                /* NATIONAL  */
    NATURAL = 549,                 /* NATURAL  */
    NCHAR = 550,                   /* NCHAR  */
    NESTED = 551,                  /* NESTED  */
    NEW = 552,                     /* NEW  */
    NEXT = 553,                    /* NEXT  */
    NFC = 554,                     /* NFC  */
    NFD = 555,                     /* NFD  */
    NFKC = 556,                    /* NFKC  */
    NFKD = 557,                    /* NFKD  */
    NO = 558,                      /* NO  */
    NOCACHE = 559,                 /* NOCACHE  */
    NOCYCLE = 560,                 /* NOCYCLE  */
    NOMAXVALUE = 561,              /* NOMAXVALUE  */
    NOMINVALUE = 562,              /* NOMINVALUE  */
    NONE = 563,                    /* NONE  */
    NOORDER = 564,                 /* NOORDER  */
    NOEXTEND = 565,                /* NOEXTEND  */
    NOKEEP = 566,                  /* NOKEEP  */
    NORMALIZE = 567,               /* NORMALIZE  */
    NORMALIZED = 568,              /* NORMALIZED  */
    NOSCALE = 569,                 /* NOSCALE  */
    NOSHARD = 570,                 /* NOSHARD  */
    NOT = 571,                     /* NOT  */
    NOTHING = 572,                 /* NOTHING  */
    NOTIFY = 573,                  /* NOTIFY  */
    NOTNULL = 574,                 /* NOTNULL  */
    NOWAIT = 575,                  /* NOWAIT  */
    NULL_P = 576,                  /* NULL_P  */
    NULLIF = 577,                  /* NULLIF  */
    NULLS_P = 578,                 /* NULLS_P  */
    NUMBER_P = 579,                /* NUMBER_P  */
    NUMERIC = 580,                 /* NUMERIC  */
    NVL = 581,                     /* NVL  */
    NVL2 = 582,                    /* NVL2  */
    OBJECT_P = 583,                /* OBJECT_P  */
    OBJECTS_P = 584,               /* OBJECTS_P  */
    OF = 585,                      /* OF  */
    OFF = 586,                     /* OFF  */
    OFFSET = 587,                  /* OFFSET  */
    OIDS = 588,                    /* OIDS  */
    OLD = 589,                     /* OLD  */
    OMIT = 590,                    /* OMIT  */
    ON = 591,                      /* ON  */
    ONLY = 592,                    /* ONLY  */
    OPERATOR = 593,                /* OPERATOR  */
    OPTION = 594,                  /* OPTION  */
    OPTIONS = 595,                 /* OPTIONS  */
    OR = 596,                      /* OR  */
    ORDER = 597,                   /* ORDER  */
    ORDINALITY = 598,              /* ORDINALITY  */
    OTHERS = 599,                  /* OTHERS  */
    OUT_P = 600,                   /* OUT_P  */
    OUTER_P = 601,                 /* OUTER_P  */
    OVER = 602,                    /* OVER  */
    OVERLAPS = 603,                /* OVERLAPS  */
    OVERLAY = 604,                 /* OVERLAY  */
    OVERRIDING = 605,              /* OVERRIDING  */
    OWNED = 606,                   /* OWNED  */
    OWNER = 607,                   /* OWNER  */
    PACKAGES = 608,                /* PACKAGES  */
    PARALLEL = 609,                /* PARALLEL  */
    PARAMETER = 610,               /* PARAMETER  */
    PARSER = 611,                  /* PARSER  */
    PARTIAL = 612,                 /* PARTIAL  */
    PARTITION = 613,               /* PARTITION  */
    PASSING = 614,                 /* PASSING  */
    PASSWORD = 615,                /* PASSWORD  */
    PATH = 616,                    /* PATH  */
    PGEXTRACT = 617,               /* PGEXTRACT  */
    PLACING = 618,                 /* PLACING  */
    PLAN = 619,                    /* PLAN  */
    PLANS = 620,                   /* PLANS  */
    POLICY = 621,                  /* POLICY  */
    POSITION = 622,                /* POSITION  */
    PRECEDING = 623,               /* PRECEDING  */
    PRECISION = 624,               /* PRECISION  */
    PRESERVE = 625,                /* PRESERVE  */
    PREPARE = 626,                 /* PREPARE  */
    PREPARED = 627,                /* PREPARED  */
    PRIMARY = 628,                 /* PRIMARY  */
    PRIOR = 629,                   /* PRIOR  */
    PRIVILEGES = 630,              /* PRIVILEGES  */
    PROCEDURAL = 631,              /* PROCEDURAL  */
    PROCEDURE = 632,               /* PROCEDURE  */
    PROCEDURES = 633,              /* PROCEDURES  */
    PROGRAM = 634,                 /* PROGRAM  */
    PUBLICATION = 635,             /* PUBLICATION  */
    QUOTE = 636,                   /* QUOTE  */
    QUOTES = 637,                  /* QUOTES  */
    RANGE = 638,                   /* RANGE  */
    READ = 639,                    /* READ  */
    REAL = 640,                    /* REAL  */
    REASSIGN = 641,                /* REASSIGN  */
    RECHECK = 642,                 /* RECHECK  */
    RECURSIVE = 643,               /* RECURSIVE  */
    REF_P = 644,                   /* REF_P  */
    REFERENCES = 645,              /* REFERENCES  */
    REFERENCING = 646,             /* REFERENCING  */
    REFRESH = 647,                 /* REFRESH  */
    REINDEX = 648,                 /* REINDEX  */
    RELATIVE_P = 649,              /* RELATIVE_P  */
    RELEASE = 650,                 /* RELEASE  */
    RENAME = 651,                  /* RENAME  */
    REPEATABLE = 652,              /* REPEATABLE  */
    REPLACE = 653,                 /* REPLACE  */
    REPLICA = 654,                 /* REPLICA  */
    RESET = 655,                   /* RESET  */
    RESTART = 656,                 /* RESTART  */
    RESTRICT = 657,                /* RESTRICT  */
    RETURN = 658,                  /* RETURN  */
    RETURNING = 659,               /* RETURNING  */
    RETURNS = 660,                 /* RETURNS  */
    REVOKE = 661,                  /* REVOKE  */
    RIGHT = 662,                   /* RIGHT  */
    ROLE = 663,                    /* ROLE  */
    ROLLBACK = 664,                /* ROLLBACK  */
    ROLLUP = 665,                  /* ROLLUP  */
    ROUTINE = 666,                 /* ROUTINE  */
    ROUTINES = 667,                /* ROUTINES  */
    ROW = 668,                     /* ROW  */
    ROWID = 669,                   /* ROWID  */
    ROWS = 670,                    /* ROWS  */
    ROWTYPE = 671,                 /* ROWTYPE  */
    RULE = 672,                    /* RULE  */
    SAVEPOINT = 673,               /* SAVEPOINT  */
    SCALAR = 674,                  /* SCALAR  */
    SCALE = 675,                   /* SCALE  */
    SCHEMA = 676,                  /* SCHEMA  */
    SCHEMAS = 677,                 /* SCHEMAS  */
    SCROLL = 678,                  /* SCROLL  */
    SEARCH = 679,                  /* SEARCH  */
    SECOND_P = 680,                /* SECOND_P  */
    SECURITY = 681,                /* SECURITY  */
    SELECT = 682,                  /* SELECT  */
    SEQUENCE = 683,                /* SEQUENCE  */
    SEQUENCES = 684,               /* SEQUENCES  */
    SERIALIZABLE = 685,            /* SERIALIZABLE  */
    SERVER = 686,                  /* SERVER  */
    SESSION = 687,                 /* SESSION  */
    SESSION_USER = 688,            /* SESSION_USER  */
    SET = 689,                     /* SET  */
    SETS = 690,                    /* SETS  */
    SETOF = 691,                   /* SETOF  */
    SHARD = 692,                   /* SHARD  */
    SHARE = 693,                   /* SHARE  */
    SHOW = 694,                    /* SHOW  */
    SIMILAR = 695,                 /* SIMILAR  */
    SIMPLE = 696,                  /* SIMPLE  */
    SKIP = 697,                    /* SKIP  */
    SMALLINT = 698,                /* SMALLINT  */
    SNAPSHOT = 699,                /* SNAPSHOT  */
    SOME = 700,                    /* SOME  */
    SOURCE = 701,                  /* SOURCE  */
    SPECIFICATION = 702,           /* SPECIFICATION  */
    SQL_P = 703,                   /* SQL_P  */
    STABLE = 704,                  /* STABLE  */
    STANDALONE_P = 705,            /* STANDALONE_P  */
    START = 706,                   /* START  */
    STATEMENT = 707,               /* STATEMENT  */
    STATISTICS = 708,              /* STATISTICS  */
    STDIN = 709,                   /* STDIN  */
    STDOUT = 710,                  /* STDOUT  */
    STORAGE = 711,                 /* STORAGE  */
    STORED = 712,                  /* STORED  */
    STRICT_P = 713,                /* STRICT_P  */
    STRING_P = 714,                /* STRING_P  */
    STRIP_P = 715,                 /* STRIP_P  */
    SUBSCRIPTION = 716,            /* SUBSCRIPTION  */
    SUBSTRING = 717,               /* SUBSTRING  */
    SUPPORT = 718,                 /* SUPPORT  */
    SYMMETRIC = 719,               /* SYMMETRIC  */
    SYSDATE = 720,                 /* SYSDATE  */
    SYSID = 721,                   /* SYSID  */
    SYSTEM_P = 722,                /* SYSTEM_P  */
    SYSTEM_USER = 723,             /* SYSTEM_USER  */
    SYSTIMESTAMP = 724,            /* SYSTIMESTAMP  */
    TABLE = 725,                   /* TABLE  */
    TABLES = 726,                  /* TABLES  */
    TABLESAMPLE = 727,             /* TABLESAMPLE  */
    TABLESPACE = 728,              /* TABLESPACE  */
    TARGET = 729,                  /* TARGET  */
    TEMP = 730,                    /* TEMP  */
    TEMPLATE = 731,                /* TEMPLATE  */
    TEMPORARY = 732,               /* TEMPORARY  */
    TEXT_P = 733,                  /* TEXT_P  */
    THEN = 734,                    /* THEN  */
    TIES = 735,                    /* TIES  */
    TIME = 736,                    /* TIME  */
    TIMESTAMP = 737,               /* TIMESTAMP  */
    TO = 738,                      /* TO  */
    TRAILING = 739,                /* TRAILING  */
    TRANSACTION = 740,             /* TRANSACTION  */
    TRANSFORM = 741,               /* TRANSFORM  */
    TREAT = 742,                   /* TREAT  */
    TRIGGER = 743,                 /* TRIGGER  */
    TRIM = 744,                    /* TRIM  */
    TRUE_P = 745,                  /* TRUE_P  */
    TRUNCATE = 746,                /* TRUNCATE  */
    TRUSTED = 747,                 /* TRUSTED  */
    TYPE_P = 748,                  /* TYPE_P  */
    TYPES_P = 749,                 /* TYPES_P  */
    UESCAPE = 750,                 /* UESCAPE  */
    UNBOUNDED = 751,               /* UNBOUNDED  */
    UNCONDITIONAL = 752,           /* UNCONDITIONAL  */
    UNCOMMITTED = 753,             /* UNCOMMITTED  */
    UNENCRYPTED = 754,             /* UNENCRYPTED  */
    UNION = 755,                   /* UNION  */
    UNIQUE = 756,                  /* UNIQUE  */
    UNKNOWN = 757,                 /* UNKNOWN  */
    UNLISTEN = 758,                /* UNLISTEN  */
    UNLOGGED = 759,                /* UNLOGGED  */
    UNTIL = 760,                   /* UNTIL  */
    UPDATE = 761,                  /* UPDATE  */
    UPDATEXML = 762,               /* UPDATEXML  */
    USER = 763,                    /* USER  */
    USERENV = 764,                 /* USERENV  */
    USING = 765,                   /* USING  */
    VACUUM = 766,                  /* VACUUM  */
    VALID = 767,                   /* VALID  */
    VALIDATE = 768,                /* VALIDATE  */
    VALIDATOR = 769,               /* VALIDATOR  */
    VALUE_P = 770,                 /* VALUE_P  */
    VALUES = 771,                  /* VALUES  */
    VARCHAR = 772,                 /* VARCHAR  */
    VARCHAR2 = 773,                /* VARCHAR2  */
    VARIADIC = 774,                /* VARIADIC  */
    VARYING = 775,                 /* VARYING  */
    VERBOSE = 776,                 /* VERBOSE  */
    VERSION_P = 777,               /* VERSION_P  */
    VIEW = 778,                    /* VIEW  */
    VIEWS = 779,                   /* VIEWS  */
    VIRTUAL = 780,                 /* VIRTUAL  */
    VISIBLE = 781,                 /* VISIBLE  */
    VOLATILE = 782,                /* VOLATILE  */
    WHEN = 783,                    /* WHEN  */
    WHERE = 784,                   /* WHERE  */
    WHITESPACE_P = 785,            /* WHITESPACE_P  */
    WINDOW = 786,                  /* WINDOW  */
    WITH = 787,                    /* WITH  */
    WITHIN = 788,                  /* WITHIN  */
    WITHOUT = 789,                 /* WITHOUT  */
    WORK = 790,                    /* WORK  */
    WRAPPER = 791,                 /* WRAPPER  */
    WRITE = 792,                   /* WRITE  */
    XML_P = 793,                   /* XML_P  */
    XMLATTRIBUTES = 794,           /* XMLATTRIBUTES  */
    XMLCONCAT = 795,               /* XMLCONCAT  */
    XMLELEMENT = 796,              /* XMLELEMENT  */
    XMLEXISTS = 797,               /* XMLEXISTS  */
    XMLFOREST = 798,               /* XMLFOREST  */
    XMLNAMESPACES = 799,           /* XMLNAMESPACES  */
    XMLPARSE = 800,                /* XMLPARSE  */
    XMLPI = 801,                   /* XMLPI  */
    XMLROOT = 802,                 /* XMLROOT  */
    XMLSERIALIZE = 803,            /* XMLSERIALIZE  */
    XMLTABLE = 804,                /* XMLTABLE  */
    YEAR_P = 805,                  /* YEAR_P  */
    YES_P = 806,                   /* YES_P  */
    ZONE = 807,                    /* ZONE  */
    EDITIONABLE = 808,             /* EDITIONABLE  */
    NONEDITIONABLE = 809,          /* NONEDITIONABLE  */
    IVYSQL = 810,                  /* IVYSQL  */
    NOCOPY = 811,                  /* NOCOPY  */
    SHARING = 812,                 /* SHARING  */
    METADATA = 813,                /* METADATA  */
    AUTHID = 814,                  /* AUTHID  */
    ACCESSIBLE = 815,              /* ACCESSIBLE  */
    PACKAGE = 816,                 /* PACKAGE  */
    DETERMINISTIC = 817,           /* DETERMINISTIC  */
    RELIES_ON = 818,               /* RELIES_ON  */
    RESULT_CACHE = 819,            /* RESULT_CACHE  */
    USING_NLS_COMP = 820,          /* USING_NLS_COMP  */
    SQL_MACRO = 821,               /* SQL_MACRO  */
    PIPELINED = 822,               /* PIPELINED  */
    POLYMORPHIC = 823,             /* POLYMORPHIC  */
    PARALLEL_ENABLE = 824,         /* PARALLEL_ENABLE  */
    HASH_P = 825,                  /* HASH_P  */
    COMPILE = 826,                 /* COMPILE  */
    DEBUG_P = 827,                 /* DEBUG_P  */
    REUSE = 828,                   /* REUSE  */
    SETTINGS = 829,                /* SETTINGS  */
    LONG_P = 830,                  /* LONG_P  */
    RAW_P = 831,                   /* RAW_P  */
    LONG_RAW = 832,                /* LONG_RAW  */
    LOOP_P = 833,                  /* LOOP_P  */
    BINARY_FLOAT_NAN = 834,        /* BINARY_FLOAT_NAN  */
    BINARY_FLOAT_INFINITY = 835,   /* BINARY_FLOAT_INFINITY  */
    BINARY_DOUBLE_NAN = 836,       /* BINARY_DOUBLE_NAN  */
    BINARY_DOUBLE_INFINITY = 837,  /* BINARY_DOUBLE_INFINITY  */
    NAN_P = 838,                   /* NAN_P  */
    INFINITE_P = 839,              /* INFINITE_P  */
    FORMAT_LA = 840,               /* FORMAT_LA  */
    NOT_LA = 841,                  /* NOT_LA  */
    NULLS_LA = 842,                /* NULLS_LA  */
    WITH_LA = 843,                 /* WITH_LA  */
    WITHOUT_LA = 844,              /* WITHOUT_LA  */
    PACKAGE_BODY = 845,            /* PACKAGE_BODY  */
    MODE_TYPE_NAME = 846,          /* MODE_TYPE_NAME  */
    MODE_PLPGSQL_EXPR = 847,       /* MODE_PLPGSQL_EXPR  */
    MODE_PLPGSQL_ASSIGN1 = 848,    /* MODE_PLPGSQL_ASSIGN1  */
    MODE_PLPGSQL_ASSIGN2 = 849,    /* MODE_PLPGSQL_ASSIGN2  */
    MODE_PLPGSQL_ASSIGN3 = 850,    /* MODE_PLPGSQL_ASSIGN3  */
    MODE_PLISQL_EXPR = 851,        /* MODE_PLISQL_EXPR  */
    MODE_PLISQL_ASSIGN1 = 852,     /* MODE_PLISQL_ASSIGN1  */
    MODE_PLISQL_ASSIGN2 = 853,     /* MODE_PLISQL_ASSIGN2  */
    MODE_PLISQL_ASSIGN3 = 854,     /* MODE_PLISQL_ASSIGN3  */
    UMINUS = 855                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 233 "ora_gram.y"

	ora_core_YYSTYPE core_yystype;
	/* these fields must match core_YYSTYPE: */
	int			ival;
	char	   *str;
	const char *keyword;

	char		chr;
	bool		boolean;
	JoinType	jtype;
	DropBehavior dbehavior;
	OnCommitAction oncommit;
	List	   *list;
	Node	   *node;
	ObjectType	objtype;
	TypeName   *typnam;
	FunctionParameter *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs *objwithargs;
	DefElem	   *defelt;
	SortBy	   *sortby;
	WindowDef  *windef;
	JoinExpr   *jexpr;
	IndexElem  *ielem;
	StatsElem  *selem;
	Alias	   *alias;
	RangeVar   *range;
	IntoClause *into;
	WithClause *with;
	InferClause	*infer;
	OnConflictClause *onconflict;
	A_Indices  *aind;
	ResTarget  *target;
	struct PrivTarget *privtarget;
	AccessPriv *accesspriv;
	struct ImportQual *importqual;
	InsertStmt *istmt;
	VariableSetStmt *vsetstmt;
	PartitionElem *partelem;
	PartitionSpec *partspec;
	PartitionBoundSpec *partboundspec;
	RoleSpec   *rolespec;
	PublicationObjSpec *publicationobjectspec;
	struct SelectLimit *selectlimit;
	SetQuantifier setquantifier;
	struct GroupClause *groupclause;
	MergeMatchKind mergematch;
	MergeWhenClause *mergewhen;
	struct KeyActions *keyactions;
	struct KeyAction *keyaction;
	ReturningClause *retclause;
	ReturningOptionKind retoptionkind;
	package_alter_flag	alter_flag;

#line 719 "ora_gram.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif




int ora_base_yyparse (ora_core_yyscan_t yyscanner);


#endif /* !YY_ORA_BASE_YY_ORA_GRAM_H_INCLUDED  */
