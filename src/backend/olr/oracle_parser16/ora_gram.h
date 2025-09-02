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
    TYPECAST = 270,                /* TYPECAST  */
    DOT_DOT = 271,                 /* DOT_DOT  */
    COLON_EQUALS = 272,            /* COLON_EQUALS  */
    EQUALS_GREATER = 273,          /* EQUALS_GREATER  */
    LESS_EQUALS = 274,             /* LESS_EQUALS  */
    GREATER_EQUALS = 275,          /* GREATER_EQUALS  */
    NOT_EQUALS = 276,              /* NOT_EQUALS  */
    LESS_LESS = 277,               /* LESS_LESS  */
    GREATER_GREATER = 278,         /* GREATER_GREATER  */
    ABORT_P = 279,                 /* ABORT_P  */
    ABSENT = 280,                  /* ABSENT  */
    ABSOLUTE_P = 281,              /* ABSOLUTE_P  */
    ACCESS = 282,                  /* ACCESS  */
    ACTION = 283,                  /* ACTION  */
    ADD_P = 284,                   /* ADD_P  */
    ADMIN = 285,                   /* ADMIN  */
    AFTER = 286,                   /* AFTER  */
    AGGREGATE = 287,               /* AGGREGATE  */
    ALL = 288,                     /* ALL  */
    ALSO = 289,                    /* ALSO  */
    ALTER = 290,                   /* ALTER  */
    ALWAYS = 291,                  /* ALWAYS  */
    ANALYSE = 292,                 /* ANALYSE  */
    ANALYZE = 293,                 /* ANALYZE  */
    AND = 294,                     /* AND  */
    ANY = 295,                     /* ANY  */
    ARRAY = 296,                   /* ARRAY  */
    AS = 297,                      /* AS  */
    ASC = 298,                     /* ASC  */
    ASENSITIVE = 299,              /* ASENSITIVE  */
    ASSERTION = 300,               /* ASSERTION  */
    ASSIGNMENT = 301,              /* ASSIGNMENT  */
    ASYMMETRIC = 302,              /* ASYMMETRIC  */
    ATOMIC = 303,                  /* ATOMIC  */
    AT = 304,                      /* AT  */
    ATTACH = 305,                  /* ATTACH  */
    ATTRIBUTE = 306,               /* ATTRIBUTE  */
    AUTHORIZATION = 307,           /* AUTHORIZATION  */
    BACKWARD = 308,                /* BACKWARD  */
    BEFORE = 309,                  /* BEFORE  */
    BEGIN_P = 310,                 /* BEGIN_P  */
    BETWEEN = 311,                 /* BETWEEN  */
    BIGINT = 312,                  /* BIGINT  */
    BINARY = 313,                  /* BINARY  */
    BINARY_DOUBLE = 314,           /* BINARY_DOUBLE  */
    BINARY_FLOAT = 315,            /* BINARY_FLOAT  */
    BIT = 316,                     /* BIT  */
    BOOLEAN_P = 317,               /* BOOLEAN_P  */
    BOTH = 318,                    /* BOTH  */
    BREADTH = 319,                 /* BREADTH  */
    BY = 320,                      /* BY  */
    BYTE_P = 321,                  /* BYTE_P  */
    CACHE = 322,                   /* CACHE  */
    CALL = 323,                    /* CALL  */
    CALLED = 324,                  /* CALLED  */
    CASCADE = 325,                 /* CASCADE  */
    CASCADED = 326,                /* CASCADED  */
    CASE = 327,                    /* CASE  */
    CAST = 328,                    /* CAST  */
    CATALOG_P = 329,               /* CATALOG_P  */
    CHAIN = 330,                   /* CHAIN  */
    CHAR_P = 331,                  /* CHAR_P  */
    CHARACTER = 332,               /* CHARACTER  */
    CHARACTERISTICS = 333,         /* CHARACTERISTICS  */
    CHECK = 334,                   /* CHECK  */
    CHECKPOINT = 335,              /* CHECKPOINT  */
    CLASS = 336,                   /* CLASS  */
    CLOSE = 337,                   /* CLOSE  */
    CLUSTER = 338,                 /* CLUSTER  */
    COALESCE = 339,                /* COALESCE  */
    COLLATE = 340,                 /* COLLATE  */
    COLLATION = 341,               /* COLLATION  */
    COLUMN = 342,                  /* COLUMN  */
    COLUMNS = 343,                 /* COLUMNS  */
    COMMENT = 344,                 /* COMMENT  */
    COMMENTS = 345,                /* COMMENTS  */
    COMMIT = 346,                  /* COMMIT  */
    COMMITTED = 347,               /* COMMITTED  */
    COMPRESSION = 348,             /* COMPRESSION  */
    CONCURRENTLY = 349,            /* CONCURRENTLY  */
    CONFIGURATION = 350,           /* CONFIGURATION  */
    CONFLICT = 351,                /* CONFLICT  */
    CONNECTION = 352,              /* CONNECTION  */
    CONSTRAINT = 353,              /* CONSTRAINT  */
    CONSTRAINTS = 354,             /* CONSTRAINTS  */
    CONTENT_P = 355,               /* CONTENT_P  */
    CONTINUE_P = 356,              /* CONTINUE_P  */
    CONVERSION_P = 357,            /* CONVERSION_P  */
    COPY = 358,                    /* COPY  */
    COST = 359,                    /* COST  */
    CREATE = 360,                  /* CREATE  */
    CROSS = 361,                   /* CROSS  */
    CSV = 362,                     /* CSV  */
    CUBE = 363,                    /* CUBE  */
    CURRENT_P = 364,               /* CURRENT_P  */
    CURRENT_CATALOG = 365,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 366,            /* CURRENT_DATE  */
    CURRENT_ROLE = 367,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 368,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 369,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 370,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 371,            /* CURRENT_USER  */
    CURSOR = 372,                  /* CURSOR  */
    CYCLE = 373,                   /* CYCLE  */
    DATA_P = 374,                  /* DATA_P  */
    DATABASE = 375,                /* DATABASE  */
    DATE_P = 376,                  /* DATE_P  */
    DAY_P = 377,                   /* DAY_P  */
    DEALLOCATE = 378,              /* DEALLOCATE  */
    DEC = 379,                     /* DEC  */
    DECIMAL_P = 380,               /* DECIMAL_P  */
    DECLARE = 381,                 /* DECLARE  */
    DECODE = 382,                  /* DECODE  */
    DEFAULT = 383,                 /* DEFAULT  */
    DEFAULTS = 384,                /* DEFAULTS  */
    DEFERRABLE = 385,              /* DEFERRABLE  */
    DEFERRED = 386,                /* DEFERRED  */
    DEFINER = 387,                 /* DEFINER  */
    DELETE_P = 388,                /* DELETE_P  */
    DELIMITER = 389,               /* DELIMITER  */
    DELIMITERS = 390,              /* DELIMITERS  */
    DEPENDS = 391,                 /* DEPENDS  */
    DEPTH = 392,                   /* DEPTH  */
    DESC = 393,                    /* DESC  */
    DETACH = 394,                  /* DETACH  */
    DICTIONARY = 395,              /* DICTIONARY  */
    DISABLE_P = 396,               /* DISABLE_P  */
    DISCARD = 397,                 /* DISCARD  */
    DISTINCT = 398,                /* DISTINCT  */
    DO = 399,                      /* DO  */
    DOCUMENT_P = 400,              /* DOCUMENT_P  */
    DOMAIN_P = 401,                /* DOMAIN_P  */
    DOUBLE_P = 402,                /* DOUBLE_P  */
    DROP = 403,                    /* DROP  */
    EACH = 404,                    /* EACH  */
    ELSE = 405,                    /* ELSE  */
    ENABLE_P = 406,                /* ENABLE_P  */
    ENCODING = 407,                /* ENCODING  */
    ENCRYPTED = 408,               /* ENCRYPTED  */
    END_P = 409,                   /* END_P  */
    ENUM_P = 410,                  /* ENUM_P  */
    ESCAPE = 411,                  /* ESCAPE  */
    EVENT = 412,                   /* EVENT  */
    EXCEPT = 413,                  /* EXCEPT  */
    EXCLUDE = 414,                 /* EXCLUDE  */
    EXCLUDING = 415,               /* EXCLUDING  */
    EXCLUSIVE = 416,               /* EXCLUSIVE  */
    EXECUTE = 417,                 /* EXECUTE  */
    EXISTS = 418,                  /* EXISTS  */
    EXPLAIN = 419,                 /* EXPLAIN  */
    EXPRESSION = 420,              /* EXPRESSION  */
    EXTEND = 421,                  /* EXTEND  */
    EXTENSION = 422,               /* EXTENSION  */
    EXTERNAL = 423,                /* EXTERNAL  */
    FALSE_P = 424,                 /* FALSE_P  */
    FAMILY = 425,                  /* FAMILY  */
    FETCH = 426,                   /* FETCH  */
    FILTER = 427,                  /* FILTER  */
    FINALIZE = 428,                /* FINALIZE  */
    FIRST_P = 429,                 /* FIRST_P  */
    FLOAT_P = 430,                 /* FLOAT_P  */
    FOLLOWING = 431,               /* FOLLOWING  */
    FOR = 432,                     /* FOR  */
    FORCE = 433,                   /* FORCE  */
    FOREIGN = 434,                 /* FOREIGN  */
    FORMAT = 435,                  /* FORMAT  */
    FORWARD = 436,                 /* FORWARD  */
    FREEZE = 437,                  /* FREEZE  */
    FROM = 438,                    /* FROM  */
    FULL = 439,                    /* FULL  */
    FUNCTION = 440,                /* FUNCTION  */
    FUNCTIONS = 441,               /* FUNCTIONS  */
    GENERATED = 442,               /* GENERATED  */
    GLOBAL = 443,                  /* GLOBAL  */
    GRANT = 444,                   /* GRANT  */
    GRANTED = 445,                 /* GRANTED  */
    GREATEST = 446,                /* GREATEST  */
    GROUP_P = 447,                 /* GROUP_P  */
    GROUPING = 448,                /* GROUPING  */
    GROUPS = 449,                  /* GROUPS  */
    HANDLER = 450,                 /* HANDLER  */
    HAVING = 451,                  /* HAVING  */
    HEADER_P = 452,                /* HEADER_P  */
    HOLD = 453,                    /* HOLD  */
    HOUR_P = 454,                  /* HOUR_P  */
    IDENTITY_P = 455,              /* IDENTITY_P  */
    IF_P = 456,                    /* IF_P  */
    ILIKE = 457,                   /* ILIKE  */
    IMMEDIATE = 458,               /* IMMEDIATE  */
    IMMUTABLE = 459,               /* IMMUTABLE  */
    IMPLICIT_P = 460,              /* IMPLICIT_P  */
    IMPORT_P = 461,                /* IMPORT_P  */
    IN_P = 462,                    /* IN_P  */
    INCLUDE = 463,                 /* INCLUDE  */
    INCLUDING = 464,               /* INCLUDING  */
    INCREMENT = 465,               /* INCREMENT  */
    INDENT = 466,                  /* INDENT  */
    INDEX = 467,                   /* INDEX  */
    INDEXES = 468,                 /* INDEXES  */
    INHERIT = 469,                 /* INHERIT  */
    INHERITS = 470,                /* INHERITS  */
    INITIALLY = 471,               /* INITIALLY  */
    INLINE_P = 472,                /* INLINE_P  */
    INNER_P = 473,                 /* INNER_P  */
    INOUT = 474,                   /* INOUT  */
    INPUT_P = 475,                 /* INPUT_P  */
    INSENSITIVE = 476,             /* INSENSITIVE  */
    INSERT = 477,                  /* INSERT  */
    INSTEAD = 478,                 /* INSTEAD  */
    INT_P = 479,                   /* INT_P  */
    INTEGER = 480,                 /* INTEGER  */
    INTERSECT = 481,               /* INTERSECT  */
    INTERVAL = 482,                /* INTERVAL  */
    INTO = 483,                    /* INTO  */
    INVOKER = 484,                 /* INVOKER  */
    IS = 485,                      /* IS  */
    ISNULL = 486,                  /* ISNULL  */
    ISOLATION = 487,               /* ISOLATION  */
    JOIN = 488,                    /* JOIN  */
    JSON = 489,                    /* JSON  */
    JSON_ARRAY = 490,              /* JSON_ARRAY  */
    JSON_ARRAYAGG = 491,           /* JSON_ARRAYAGG  */
    JSON_OBJECT = 492,             /* JSON_OBJECT  */
    JSON_OBJECTAGG = 493,          /* JSON_OBJECTAGG  */
    KEEP = 494,                    /* KEEP  */
    KEY = 495,                     /* KEY  */
    KEYS = 496,                    /* KEYS  */
    LABEL = 497,                   /* LABEL  */
    LANGUAGE = 498,                /* LANGUAGE  */
    LARGE_P = 499,                 /* LARGE_P  */
    LAST_P = 500,                  /* LAST_P  */
    LATERAL_P = 501,               /* LATERAL_P  */
    LEADING = 502,                 /* LEADING  */
    LEAKPROOF = 503,               /* LEAKPROOF  */
    LEAST = 504,                   /* LEAST  */
    LEFT = 505,                    /* LEFT  */
    LEVEL = 506,                   /* LEVEL  */
    LIKE = 507,                    /* LIKE  */
    LIMIT = 508,                   /* LIMIT  */
    LISTEN = 509,                  /* LISTEN  */
    LOAD = 510,                    /* LOAD  */
    LOCAL = 511,                   /* LOCAL  */
    LOCALTIME = 512,               /* LOCALTIME  */
    LOCALTIMESTAMP = 513,          /* LOCALTIMESTAMP  */
    LOCATION = 514,                /* LOCATION  */
    LOCK_P = 515,                  /* LOCK_P  */
    LOCKED = 516,                  /* LOCKED  */
    LOGGED = 517,                  /* LOGGED  */
    MAPPING = 518,                 /* MAPPING  */
    MATCH = 519,                   /* MATCH  */
    MATCHED = 520,                 /* MATCHED  */
    MATERIALIZED = 521,            /* MATERIALIZED  */
    MAXVALUE = 522,                /* MAXVALUE  */
    MERGE = 523,                   /* MERGE  */
    METHOD = 524,                  /* METHOD  */
    MINUTE_P = 525,                /* MINUTE_P  */
    MINVALUE = 526,                /* MINVALUE  */
    MODE = 527,                    /* MODE  */
    MODIFY = 528,                  /* MODIFY  */
    MONTH_P = 529,                 /* MONTH_P  */
    MOVE = 530,                    /* MOVE  */
    NAME_P = 531,                  /* NAME_P  */
    NAMES = 532,                   /* NAMES  */
    NATIONAL = 533,                /* NATIONAL  */
    NATURAL = 534,                 /* NATURAL  */
    NCHAR = 535,                   /* NCHAR  */
    NEW = 536,                     /* NEW  */
    NEXT = 537,                    /* NEXT  */
    NFC = 538,                     /* NFC  */
    NFD = 539,                     /* NFD  */
    NFKC = 540,                    /* NFKC  */
    NFKD = 541,                    /* NFKD  */
    NO = 542,                      /* NO  */
    NOCACHE = 543,                 /* NOCACHE  */
    NOCYCLE = 544,                 /* NOCYCLE  */
    NOMAXVALUE = 545,              /* NOMAXVALUE  */
    NOMINVALUE = 546,              /* NOMINVALUE  */
    NONE = 547,                    /* NONE  */
    NOORDER = 548,                 /* NOORDER  */
    NOEXTEND = 549,                /* NOEXTEND  */
    NOKEEP = 550,                  /* NOKEEP  */
    NORMALIZE = 551,               /* NORMALIZE  */
    NORMALIZED = 552,              /* NORMALIZED  */
    NOSCALE = 553,                 /* NOSCALE  */
    NOSHARD = 554,                 /* NOSHARD  */
    NOT = 555,                     /* NOT  */
    NOTHING = 556,                 /* NOTHING  */
    NOTIFY = 557,                  /* NOTIFY  */
    NOTNULL = 558,                 /* NOTNULL  */
    NOWAIT = 559,                  /* NOWAIT  */
    NULL_P = 560,                  /* NULL_P  */
    NULLIF = 561,                  /* NULLIF  */
    NULLS_P = 562,                 /* NULLS_P  */
    NUMBER_P = 563,                /* NUMBER_P  */
    NUMERIC = 564,                 /* NUMERIC  */
    NVL = 565,                     /* NVL  */
    NVL2 = 566,                    /* NVL2  */
    OBJECT_P = 567,                /* OBJECT_P  */
    OF = 568,                      /* OF  */
    OFF = 569,                     /* OFF  */
    OFFSET = 570,                  /* OFFSET  */
    OIDS = 571,                    /* OIDS  */
    OLD = 572,                     /* OLD  */
    ON = 573,                      /* ON  */
    ONLY = 574,                    /* ONLY  */
    OPERATOR = 575,                /* OPERATOR  */
    OPTION = 576,                  /* OPTION  */
    OPTIONS = 577,                 /* OPTIONS  */
    OR = 578,                      /* OR  */
    ORDER = 579,                   /* ORDER  */
    ORDINALITY = 580,              /* ORDINALITY  */
    OTHERS = 581,                  /* OTHERS  */
    OUT_P = 582,                   /* OUT_P  */
    OUTER_P = 583,                 /* OUTER_P  */
    OVER = 584,                    /* OVER  */
    OVERLAPS = 585,                /* OVERLAPS  */
    OVERLAY = 586,                 /* OVERLAY  */
    OVERRIDING = 587,              /* OVERRIDING  */
    OWNED = 588,                   /* OWNED  */
    OWNER = 589,                   /* OWNER  */
    PARALLEL = 590,                /* PARALLEL  */
    PARAMETER = 591,               /* PARAMETER  */
    PARSER = 592,                  /* PARSER  */
    PARTIAL = 593,                 /* PARTIAL  */
    PARTITION = 594,               /* PARTITION  */
    PASSING = 595,                 /* PASSING  */
    PASSWORD = 596,                /* PASSWORD  */
    PGEXTRACT = 597,               /* PGEXTRACT  */
    PLACING = 598,                 /* PLACING  */
    PLANS = 599,                   /* PLANS  */
    POLICY = 600,                  /* POLICY  */
    POSITION = 601,                /* POSITION  */
    PRECEDING = 602,               /* PRECEDING  */
    PRECISION = 603,               /* PRECISION  */
    PRESERVE = 604,                /* PRESERVE  */
    PREPARE = 605,                 /* PREPARE  */
    PREPARED = 606,                /* PREPARED  */
    PRIMARY = 607,                 /* PRIMARY  */
    PRIOR = 608,                   /* PRIOR  */
    PRIVILEGES = 609,              /* PRIVILEGES  */
    PROCEDURAL = 610,              /* PROCEDURAL  */
    PROCEDURE = 611,               /* PROCEDURE  */
    PROCEDURES = 612,              /* PROCEDURES  */
    PROGRAM = 613,                 /* PROGRAM  */
    PUBLICATION = 614,             /* PUBLICATION  */
    QUOTE = 615,                   /* QUOTE  */
    RANGE = 616,                   /* RANGE  */
    READ = 617,                    /* READ  */
    REAL = 618,                    /* REAL  */
    REASSIGN = 619,                /* REASSIGN  */
    RECHECK = 620,                 /* RECHECK  */
    RECURSIVE = 621,               /* RECURSIVE  */
    REF_P = 622,                   /* REF_P  */
    REFERENCES = 623,              /* REFERENCES  */
    REFERENCING = 624,             /* REFERENCING  */
    REFRESH = 625,                 /* REFRESH  */
    REINDEX = 626,                 /* REINDEX  */
    RELATIVE_P = 627,              /* RELATIVE_P  */
    RELEASE = 628,                 /* RELEASE  */
    RENAME = 629,                  /* RENAME  */
    REPEATABLE = 630,              /* REPEATABLE  */
    REPLACE = 631,                 /* REPLACE  */
    REPLICA = 632,                 /* REPLICA  */
    RESET = 633,                   /* RESET  */
    RESTART = 634,                 /* RESTART  */
    RESTRICT = 635,                /* RESTRICT  */
    RETURN = 636,                  /* RETURN  */
    RETURNING = 637,               /* RETURNING  */
    RETURNS = 638,                 /* RETURNS  */
    REVOKE = 639,                  /* REVOKE  */
    RIGHT = 640,                   /* RIGHT  */
    ROLE = 641,                    /* ROLE  */
    ROLLBACK = 642,                /* ROLLBACK  */
    ROLLUP = 643,                  /* ROLLUP  */
    ROUTINE = 644,                 /* ROUTINE  */
    ROUTINES = 645,                /* ROUTINES  */
    ROW = 646,                     /* ROW  */
    ROWS = 647,                    /* ROWS  */
    ROWTYPE = 648,                 /* ROWTYPE  */
    RULE = 649,                    /* RULE  */
    SAVEPOINT = 650,               /* SAVEPOINT  */
    SCALAR = 651,                  /* SCALAR  */
    SCALE = 652,                   /* SCALE  */
    SCHEMA = 653,                  /* SCHEMA  */
    SCHEMAS = 654,                 /* SCHEMAS  */
    SCROLL = 655,                  /* SCROLL  */
    SEARCH = 656,                  /* SEARCH  */
    SECOND_P = 657,                /* SECOND_P  */
    SECURITY = 658,                /* SECURITY  */
    SELECT = 659,                  /* SELECT  */
    SEQUENCE = 660,                /* SEQUENCE  */
    SEQUENCES = 661,               /* SEQUENCES  */
    SERIALIZABLE = 662,            /* SERIALIZABLE  */
    SERVER = 663,                  /* SERVER  */
    SESSION = 664,                 /* SESSION  */
    SESSION_USER = 665,            /* SESSION_USER  */
    SET = 666,                     /* SET  */
    SETS = 667,                    /* SETS  */
    SETOF = 668,                   /* SETOF  */
    SHARD = 669,                   /* SHARD  */
    SHARE = 670,                   /* SHARE  */
    SHOW = 671,                    /* SHOW  */
    SIMILAR = 672,                 /* SIMILAR  */
    SIMPLE = 673,                  /* SIMPLE  */
    SKIP = 674,                    /* SKIP  */
    SMALLINT = 675,                /* SMALLINT  */
    SNAPSHOT = 676,                /* SNAPSHOT  */
    SOME = 677,                    /* SOME  */
    SQL_P = 678,                   /* SQL_P  */
    STABLE = 679,                  /* STABLE  */
    STANDALONE_P = 680,            /* STANDALONE_P  */
    START = 681,                   /* START  */
    STATEMENT = 682,               /* STATEMENT  */
    STATISTICS = 683,              /* STATISTICS  */
    STDIN = 684,                   /* STDIN  */
    STDOUT = 685,                  /* STDOUT  */
    STORAGE = 686,                 /* STORAGE  */
    STORED = 687,                  /* STORED  */
    STRICT_P = 688,                /* STRICT_P  */
    STRIP_P = 689,                 /* STRIP_P  */
    SUBSCRIPTION = 690,            /* SUBSCRIPTION  */
    SUBSTRING = 691,               /* SUBSTRING  */
    SUPPORT = 692,                 /* SUPPORT  */
    SYMMETRIC = 693,               /* SYMMETRIC  */
    SYSDATE = 694,                 /* SYSDATE  */
    SYSID = 695,                   /* SYSID  */
    SYSTEM_P = 696,                /* SYSTEM_P  */
    SYSTEM_USER = 697,             /* SYSTEM_USER  */
    SYSTIMESTAMP = 698,            /* SYSTIMESTAMP  */
    TABLE = 699,                   /* TABLE  */
    TABLES = 700,                  /* TABLES  */
    TABLESAMPLE = 701,             /* TABLESAMPLE  */
    TABLESPACE = 702,              /* TABLESPACE  */
    TEMP = 703,                    /* TEMP  */
    TEMPLATE = 704,                /* TEMPLATE  */
    TEMPORARY = 705,               /* TEMPORARY  */
    TEXT_P = 706,                  /* TEXT_P  */
    THEN = 707,                    /* THEN  */
    TIES = 708,                    /* TIES  */
    TIME = 709,                    /* TIME  */
    TIMESTAMP = 710,               /* TIMESTAMP  */
    TO = 711,                      /* TO  */
    TRAILING = 712,                /* TRAILING  */
    TRANSACTION = 713,             /* TRANSACTION  */
    TRANSFORM = 714,               /* TRANSFORM  */
    TREAT = 715,                   /* TREAT  */
    TRIGGER = 716,                 /* TRIGGER  */
    TRIM = 717,                    /* TRIM  */
    TRUE_P = 718,                  /* TRUE_P  */
    TRUNCATE = 719,                /* TRUNCATE  */
    TRUSTED = 720,                 /* TRUSTED  */
    TYPE_P = 721,                  /* TYPE_P  */
    TYPES_P = 722,                 /* TYPES_P  */
    UESCAPE = 723,                 /* UESCAPE  */
    UNBOUNDED = 724,               /* UNBOUNDED  */
    UNCOMMITTED = 725,             /* UNCOMMITTED  */
    UNENCRYPTED = 726,             /* UNENCRYPTED  */
    UNION = 727,                   /* UNION  */
    UNIQUE = 728,                  /* UNIQUE  */
    UNKNOWN = 729,                 /* UNKNOWN  */
    UNLISTEN = 730,                /* UNLISTEN  */
    UNLOGGED = 731,                /* UNLOGGED  */
    UNTIL = 732,                   /* UNTIL  */
    UPDATE = 733,                  /* UPDATE  */
    UPDATEXML = 734,               /* UPDATEXML  */
    USER = 735,                    /* USER  */
    USERENV = 736,                 /* USERENV  */
    USING = 737,                   /* USING  */
    VACUUM = 738,                  /* VACUUM  */
    VALID = 739,                   /* VALID  */
    VALIDATE = 740,                /* VALIDATE  */
    VALIDATOR = 741,               /* VALIDATOR  */
    VALUE_P = 742,                 /* VALUE_P  */
    VALUES = 743,                  /* VALUES  */
    VARCHAR = 744,                 /* VARCHAR  */
    VARCHAR2 = 745,                /* VARCHAR2  */
    VARIADIC = 746,                /* VARIADIC  */
    VARYING = 747,                 /* VARYING  */
    VERBOSE = 748,                 /* VERBOSE  */
    VERSION_P = 749,               /* VERSION_P  */
    VIEW = 750,                    /* VIEW  */
    VIEWS = 751,                   /* VIEWS  */
    VOLATILE = 752,                /* VOLATILE  */
    WHEN = 753,                    /* WHEN  */
    WHERE = 754,                   /* WHERE  */
    WHITESPACE_P = 755,            /* WHITESPACE_P  */
    WINDOW = 756,                  /* WINDOW  */
    WITH = 757,                    /* WITH  */
    WITHIN = 758,                  /* WITHIN  */
    WITHOUT = 759,                 /* WITHOUT  */
    WORK = 760,                    /* WORK  */
    WRAPPER = 761,                 /* WRAPPER  */
    WRITE = 762,                   /* WRITE  */
    XML_P = 763,                   /* XML_P  */
    XMLATTRIBUTES = 764,           /* XMLATTRIBUTES  */
    XMLCONCAT = 765,               /* XMLCONCAT  */
    XMLELEMENT = 766,              /* XMLELEMENT  */
    XMLEXISTS = 767,               /* XMLEXISTS  */
    XMLFOREST = 768,               /* XMLFOREST  */
    XMLNAMESPACES = 769,           /* XMLNAMESPACES  */
    XMLPARSE = 770,                /* XMLPARSE  */
    XMLPI = 771,                   /* XMLPI  */
    XMLROOT = 772,                 /* XMLROOT  */
    XMLSERIALIZE = 773,            /* XMLSERIALIZE  */
    XMLTABLE = 774,                /* XMLTABLE  */
    YEAR_P = 775,                  /* YEAR_P  */
    YES_P = 776,                   /* YES_P  */
    ZONE = 777,                    /* ZONE  */
    EDITIONABLE = 778,             /* EDITIONABLE  */
    NONEDITIONABLE = 779,          /* NONEDITIONABLE  */
    IVYSQL = 780,                  /* IVYSQL  */
    NOCOPY = 781,                  /* NOCOPY  */
    SHARING = 782,                 /* SHARING  */
    METADATA = 783,                /* METADATA  */
    AUTHID = 784,                  /* AUTHID  */
    ACCESSIBLE = 785,              /* ACCESSIBLE  */
    PACKAGE = 786,                 /* PACKAGE  */
    DETERMINISTIC = 787,           /* DETERMINISTIC  */
    RELIES_ON = 788,               /* RELIES_ON  */
    RESULT_CACHE = 789,            /* RESULT_CACHE  */
    USING_NLS_COMP = 790,          /* USING_NLS_COMP  */
    SQL_MACRO = 791,               /* SQL_MACRO  */
    PIPELINED = 792,               /* PIPELINED  */
    POLYMORPHIC = 793,             /* POLYMORPHIC  */
    PARALLEL_ENABLE = 794,         /* PARALLEL_ENABLE  */
    HASH_P = 795,                  /* HASH_P  */
    COMPILE = 796,                 /* COMPILE  */
    DEBUG_P = 797,                 /* DEBUG_P  */
    REUSE = 798,                   /* REUSE  */
    SETTINGS = 799,                /* SETTINGS  */
    LONG_P = 800,                  /* LONG_P  */
    RAW_P = 801,                   /* RAW_P  */
    LONG_RAW = 802,                /* LONG_RAW  */
    LOOP_P = 803,                  /* LOOP_P  */
    BINARY_FLOAT_NAN = 804,        /* BINARY_FLOAT_NAN  */
    BINARY_FLOAT_INFINITY = 805,   /* BINARY_FLOAT_INFINITY  */
    BINARY_DOUBLE_NAN = 806,       /* BINARY_DOUBLE_NAN  */
    BINARY_DOUBLE_INFINITY = 807,  /* BINARY_DOUBLE_INFINITY  */
    NAN_P = 808,                   /* NAN_P  */
    INFINITE_P = 809,              /* INFINITE_P  */
    FORMAT_LA = 810,               /* FORMAT_LA  */
    NOT_LA = 811,                  /* NOT_LA  */
    NULLS_LA = 812,                /* NULLS_LA  */
    WITH_LA = 813,                 /* WITH_LA  */
    WITHOUT_LA = 814,              /* WITHOUT_LA  */
    MODE_TYPE_NAME = 815,          /* MODE_TYPE_NAME  */
    MODE_PLPGSQL_EXPR = 816,       /* MODE_PLPGSQL_EXPR  */
    MODE_PLPGSQL_ASSIGN1 = 817,    /* MODE_PLPGSQL_ASSIGN1  */
    MODE_PLPGSQL_ASSIGN2 = 818,    /* MODE_PLPGSQL_ASSIGN2  */
    MODE_PLPGSQL_ASSIGN3 = 819,    /* MODE_PLPGSQL_ASSIGN3  */
    UMINUS = 820                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 245 "ora_gram.y"

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
	MergeWhenClause *mergewhen;
	struct KeyActions *keyactions;
	struct KeyAction *keyaction;

#line 680 "ora_gram.h"

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
