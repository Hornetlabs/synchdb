# 字母大小寫策略

## 設定字母大小寫策略

字母大小寫策略決定了 SynchDB 在將來源物件名稱套用到目標資料庫之前應如何標準化。 MySQL、SQL Server 和 PostgreSQL 預設將所有物件名稱轉換為小寫字母，而 Oracle 預設轉換為大寫字母。 MySQL、Oracle 和 PostgreSQL 允許物件名稱包含大小寫字母混合，並將每種變體視為不同的名稱，而 SQL Server 則將它們視為相同的名稱。

可以使用 `synchdb.letter_casing_strategy` GUC 參數來告訴 SynchDB 如何處理物件名稱，其值如下：

* lowercase：將所有物件名稱（表名和列名）標準化為小寫字母

* uppercase：將所有物件名稱（表名和列名）規範化為大寫字母

* asis：不進行規範化，保持物件名稱（表名和列名）不變。

<<重要>> synchdb.letter_casing_strategy 不會影響資料型別名稱。在 SynchDB 中，資料類型名稱始終規範化為小寫字母。像 SQL Server 和 Oracle Express 這樣的連接器使用大寫字母作為資料類型，因此 SynchDB 會將其儲存和顯示為小寫。

## 字母大小寫策略如何影響轉換

您可能想知道，如果 SynchDB 根據 `synchdb.letter_casing_strategy` 對物件名稱進行規範化，那麼如何正確配置 [物件轉換](../../user-guide/object_mapping_rules/) 以識別要轉換的外部表或列？

物件轉換會識別來源資料庫中用於轉換的原始名稱，因此您必須按照來源資料庫中的名稱配置物件名稱，而不是 SynchDB 規範化後的名稱。

如果找到匹配項，SynchDB 將優先使用物件轉換識別出的新值，而不會根據字母大小寫策略對其進行規範化。當沒有為物件定義特定的轉換規則時，就會發生規範化。

例如，對於 Oracle 資料庫“FREE”下模式“DBZUSER”中的表“TEST_TABLE”，假設我們要將其對應到 PostgreSQL 中的“MY_TEST_TABLE”（全部大寫），而目前的 synchdb.letter_casing_strategy 設定為“lowercase”。您可以這樣寫轉換規則：

```sql

SELECT synchdb_add_objmap('mysqlconn','table','FREE.DBZUSER.TEST_TABLE','MY_TEST_TABLE');

```

由於我們已經為「FREE.DBZUSER.TEST_TABLE」定義了轉換規則，因此該轉換規則將優先執行，並按照指示將其轉換為「MY_TEST_TABLE」。字母大小寫策略（lowercase）不會將其標準化為小寫。