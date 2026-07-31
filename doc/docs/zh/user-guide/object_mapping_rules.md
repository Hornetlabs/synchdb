# 配置对象映射和转换规则

SynchDB 具有默认的名称和数据类型映射规则来处理传入的更改事件。大多数情况下，默认规则即可正常工作。但是，如果您有特定的转换要求，或者默认规则不适合您，则可以为特定连接器配置自己的对象映射规则。

## **synchdb_add_objmap**

此实用函数可用于配置表名、列名、数据类型以及转换规则。它需要 4 个参数：

| 参数 | 描述 | 必需 | 示例 | 备注 |
|:-:|:-|:-:|:-|:-|
| `name` | 此连接器的唯一标识符 | ✓ | `'mysqlconn'` | 必须在所有连接器中唯一 |
| `object type` | 对象映射的类型 | ✓ | `'table'` |可以是 `table` 来映射表名，`column` 来映射列名，`datatype` 来映射数据类型，或 `transform` 来运行数据转换表达式 |
| `source object` | 以完全限定名称表示的源对象 | ✓ | `inventory.customers` | 远程数据库中的对象名称 |
| `destination object` | 目标对象名称 | ✓ | `'schema1.people'` | PostgreSQL 端的目标对象名称。可以是完全限定表名、列名、数据类型或转换表达式 |

## **配置表名映射**

* `source object` 表示远程数据库中以完全限定名称表示的表
* `destination object` 表示 PostgreSQL 中的表名。它可以只是一个名称（默认为公共架构）或 schema.name 格式。

此示例将源数据库中的 `inventory.customers` 表映射到 PostgreSQL 中的 `schema1.people`。
```sql
SELECT synchdb_add_objmap('mysqlconn','table','inventory.customers','schema1.people');
```

## **配置列名映射**

* `源对象` 表示远程数据库中完全限定名称的列
* `目标对象` 表示 PostgreSQL 中的列名。无需将其格式化为完全限定列名。

此示例将源表中的 `inventory.customers.email` 列映射到 PostgreSQL 中的 `contact`。
```sql
SELECT synchdb_add_objmap('mysqlconn','column','inventory.customers.email','contact');
```

## **配置数据类型映射**

* `源对象` 可以表示为以下之一：
* 完全限定列 (inventory.geom.g)。这意味着数据类型映射仅适用于此特定列。
* 通用数据类型字符串 (int)。如果是自增数据类型，请使用竖线 (|) 添加（int|true 表示自增 int），否则使用竖线 (|) 添加（int|false 表示非自增 int）。这意味着数据类型映射适用于所有符合条件的数据类型。

* `destination object` 应表示为 PostgreSQL 中存在的通用数据类型字符串。使用竖线 (|) 覆盖大小（text|0 将大小覆盖为 0，因为 text 是可变大小）或（varchar|-1 使用 change 事件附带的任何大小）。

此示例将所有非自增 `point` 数据类型映射到 PostgreSQL 中的 `text` 数据类型。
```sql
SELECT synchdb_add_objmap('mysqlconn','datatype','point|false','text|0');
```

此示例将表 `inventory.geom` 的列 `g` 的数据类型映射到 PostgreSQL 中的 `geometry`。
```sql
SELECT synchdb_add_objmap('mysqlconn','datatype','inventory.geom.g','geometry|0');
```

## **配置转换规则**

* `源对象` 表示要转换的列
* `目标对象` 表示在将列数据应用于 PostgreSQL 之前要对其运行的表达式。使用 %d 作为输入列数据的占位符。如果是几何类型，则使用 %w 表示 WKB，%s 表示 SRID。

此示例将在 SynchDB 收到的“inventory.products.name”列的值前添加“>>>>>”并在其后添加“<<<<<”。
```sql
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.products.name','''>>>>'' || ''%d'' || ''<<<<<''');
```

此示例将始终在 SynchDB 收到的“inventory.orders.quantity”列的值后添加 500：
```sql
SELECT synchdb_add_objmap('mysqlconn','transform','inventory.orders.quantity','%d + 500');
```

## ** 使用规则**

如果连接器未运行，规则会在下次启动时通过 `synchdb_start_engine_bgw` 自动应用。

如果连接器已在运行，规则不会自动应用，我们必须告诉连接器重新加载对象映射规则，并使用实用函数 `synchdb_reload_objmap` 来应用规则。

```sql
SELECT synchdb_reload_objmap('mysqlconn');
```