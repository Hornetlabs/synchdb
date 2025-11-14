#ifndef NODETAGS_EXT_H
#define NODETAGS_EXT_H

/*
 * Custom NodeTag values for IvorySQL or Oracle parser
 * Make sure these start AFTER the last vanilla PostgreSQL NodeTag
 */
#define T_OraNodetagBegin 600

#define T_AccessibleByClause       (T_OraNodetagBegin + 1)
#define T_AccessorItem             (T_OraNodetagBegin + 2)
#define T_CompileFunctionStmt      (T_OraNodetagBegin + 3)
#define T_CreatePackageStmt        (T_OraNodetagBegin + 4)
#define T_CreatePackageBodyStmt    (T_OraNodetagBegin + 5)
#define T_AlterPackageStmt         (T_OraNodetagBegin + 6)
#define T_ColumnRefOrFuncCall      (T_OraNodetagBegin + 7)
#define T_BFloat                   (T_OraNodetagBegin + 8)
#define T_BDouble                  (T_OraNodetagBegin + 9)
#define T_OraParamRef              (T_OraNodetagBegin + 10)

#endif  /* NODETAGS_EXT_H */

