# 处理非原生数据类型

## **处理非原生数据类型**
表可能包含由用户自定义创建或由另一个已安装的扩展创建的列数据类型。在这种情况下，之前提到的原生数据类型处理将不起作用，因为该类型未列在受支持的原生数据类型列表中。相反，DML 转换器会访问PG Catalog 目录，获取非原生数据类型的 OID，并查找其在 PostgreSQL 中定义的“类别”。以下是截至版本 17 的 PostgreSQL 支持的类别列表：

```
#define  TYPCATEGORY_INVALID  '\0'
#define  TYPCATEGORY_ARRAY    'A'
#define  TYPCATEGORY_BOOLEAN  'B'
#define  TYPCATEGORY_COMPOSITE  'C'
#define  TYPCATEGORY_DATETIME 'D'
#define  TYPCATEGORY_ENUM   'E'
#define  TYPCATEGORY_GEOMETRIC  'G'
#define  TYPCATEGORY_NETWORK  'I'
#define  TYPCATEGORY_NUMERIC  'N'
#define  TYPCATEGORY_PSEUDOTYPE 'P'
#define  TYPCATEGORY_RANGE    'R'
#define  TYPCATEGORY_STRING   'S'
#define  TYPCATEGORY_TIMESPAN 'T'
#define  TYPCATEGORY_USER   'U'
#define  TYPCATEGORY_BITSTRING  'V'
#define  TYPCATEGORY_UNKNOWN  'X'

```

类别会告诉 DML 转换器数据类型的性质（数字？字符串？日期时间？...等等），以帮助转换器选择正确的例程进行处理。在大多数情况下，使用类别与描述输入数据格式的 DBZ 元数据配对就足以选择正确的例程来处理数据。但是，在某些情况下，这可能还不够。例如，自定义 DATE、TIME、TIMESTAMP 日期类型都可以归类在 `TYPCATEGORY_DATETIME` 下，因此转换器不知道它是否正在使用 DATE、TIME 或 TIMESTAMP，因为每个类型都会产生不同的时间格式。目前，转换器会从数据类型名称中查找某些关键字来识别。将来，我们可能会公开这部分，让用户告诉转换器在出现歧义时应该使用哪个例程。另一个示例是 `TYPCATEGORY_USER` 和 `TYPCATEGORY_GEOMETRIC`，它们没有清楚地表明数据格式。对于这些类别，转换器目前不执行任何进一步处理，因为它只是将数据负载保持原样。PostgreSQL 可能会也可能不会拒绝此类未处理的数据。这就是为什么接下来的转换功能很重要，它为 DML 转换器提供了最后一次纠正其数据负载的机会。