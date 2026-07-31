# JMX MBEAN 监控

## **JMX（Java 管理扩展）**

JMX，即 Java 管理扩展，是一种 Java 技术，提供用于管理和监控应用程序、系统对象、设备和面向服务的网络的工具。它允许开发人员公开应用程序信息，并允许外部工具访问和交互这些信息，以进行监控和管理。

## **Debezium 的 JMX MBean**
Debezium 连接器通过连接器的 MBean 名称（MBean 标签等于连接器名称）公开指标。这些指标特定于每个连接器实例，提供有关连接器快照、流式传输和模式历史记录进程行为的数据。

## **使用 synchdb_add_jmx_conninfo() 在连接器上启用 JMX，或使用 synchdb_del_jmx_conninfo() 禁用**
synchdb_add_jmx_conninfo() 和 synchdb_del_jmx_conninfo() 函数用于向现有连接器添加或删除 JMX 监控配置。这允许通过 JConsole 等工具进行运行时监控和诊断。

**函数签名**

```
synchdb_add_jmx_conninfo(
    name text,
    jmx_listenaddr text,
    jmx_port integer,
    jmx_rmiserveraddr text,
    jmx_rmiport integer,
    jmx_auth boolean,
    jmx_auth_passwdfile text,
    jmx_auth_accessfile text,
    jmx_ssl boolean,
    jmx_ssl_keystore text,
    jmx_ssl_keystore_pass text,
    jmx_ssl_truststore text,
    jmx_ssl_truststore_pass text
)

synchdb_del_jmx_conninfo(
    name text
)

```

| 参数 | 描述 |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`name`** | *(文本)*<br>要添加 JMX 配置的现有连接器的名称。必须已存在于 `synchdb_conninfo` 中。|
| **`jmx_listenaddr`** | *(文本)*<br>JVM 将在其上侦听 JMX 连接的 IP 地址。使用 `'0.0.0.0'` 侦听所有网络接口，或指定特定 IP。|
| **`jmx_port`** | *(整数)*<br>JMX 通信的本地端口（用于客户端发现）。必须处于打开状态且未使用。|
| **`jmx_rmiserveraddr`** | *(文本)*<br>远程 JMX 客户端（例如 JConsole）用于连接 JVM 的公共/外部 IP 地址。通常设置为主机的外部 IP。|
| **`jmx_rmiport`** | *(整数)*<br>用于实际 JMX 对象通信的 RMI 服务器端口。此端口也必须在防火墙和网络设置中打开。通常与 `jmx_port` 相同。|
| **`jmx_auth`** | *(布尔值)*<br>是否启用 JMX 身份验证？如果为 `true`，客户端必须提供存储在以下文件中的用户名/密码。|
| **`jmx_auth_passwdfile`** | *(文本)*<br>用于 JMX 身份验证的密码文件的路径。如果不使用身份验证，请传递 `'null'`。|
| **`jmx_auth_accessfile`** | *(text)*<br>定义角色（例如 `monitor`、`control`）的访问控制文件的路径。仅在启用身份验证时才需要。如果不使用身份验证，请传递 `'null'` |
| **`jmx_ssl`** | *(boolean)*<br>是否为 JMX 连接启用 SSL 加密？设置为 `true` 以确保通信安全。|
| **`jmx_ssl_keystore`** | *(text)*<br>包含服务器私钥和证书的密钥库文件（JKS 格式）的路径。如果 `jmx_ssl = true`，则为必填项。|
| **`jmx_ssl_keystore_pass`** | *(text)*<br>密钥库文件的密码。|
| **`jmx_ssl_truststore`** | *(text)*<br>保存受信任 CA 证书的信任库 (JKS) 的路径。如果配置了双向 TLS，则用于验证客户端身份。|
| **`jmx_ssl_truststore_pass`** | *(文本)*<br>信任库文件的密码。

## **JMX 身份验证的密码和访问文件**

在 JVM 配置中启用 JMX 身份验证（即设置 jmx_auth = true）时，您必须提供两个文件：

### **密码文件**：

此文件存储有效的 JMX 用户名及其对应的密码。

**格式：**

```
# Format: username password
<username> <password>

```

**例子：**

```
monitorRole mySecretPassword
controlRole anotherSecretPassword

```

### **访问文件**：

此文件定义了每个用户可以通过 JMX 执行的操作。可能的访问级别：

* readonly – 可以查看 MBean，但不能修改。
* readwrite – 可以查看和修改 MBean。

格式：
```
# 格式：用户名 访问级别
<用户名> <访问权限>

```

示例：
```
monitorRole readonly
controlRole readwrite

```

### **文件权限**：

确保两个文件均归运行 JVM 的用户所有，且具有受限的权限：

```
chmod 600 jmxpwd.file jmxacc.file
chown youruser:youruser jmxpwd.file jmxacc.file

```

## **synchdb_add_jmx_conninfo 示例**

**启用无需身份验证和 SSL 的 JMX MBean：**

```sql
SELECT synchdb_add_jmx_conninfo(
	'mysqlconn',	-- existing connector name
	'0.0.0.0',		-- JMX listen address
	9010,			-- JMX listen port
	'10.55.13.17',	-- JMX RMI server address - for remote client connect
	9010,			-- JMX RMI server port - for remoet client connect
	false,			-- use authentication?
	'null',			-- password file for authentication
	'null',			-- access file for authentication
	false,			-- use SSL?
	'null',			-- SSL keystore path
	'null',			-- SSL keystore password
	'null',			-- SSL trust store
	'null');		-- SSL trust store password
```

**启用带有身份验证的 JMX MBean**

```sql
SELECT synchdb_add_jmx_conninfo(
	'mysqlconn',	-- existing connector name
	'0.0.0.0',		-- JMX listen address
	9010,			-- JMX listen port
	'10.55.13.17',	-- JMX RMI server address - for remote client connect
	9010,			-- JMX RMI server port - for remoet client connect
	true,			-- use authentication?
	'/path/to/passwd.file',			-- password file for authentication
	'/path/to/access.file',			-- access file for authentication
	false,			-- use SSL?
	'null',			-- SSL keystore path
	'null',			-- SSL keystore password
	'null',			-- SSL trust store
	'null');		-- SSL trust store password
```

**启用带有身份验证和 SSL 的 JMX MBean，无需 SSL 客户端验证**

```sql
SELECT synchdb_add_jmx_conninfo(
	'mysqlconn',	-- existing connector name
	'0.0.0.0',		-- JMX listen address
	9010,			-- JMX listen port
	'10.55.13.17',	-- JMX RMI server address - for remote client connect
	9010,			-- JMX RMI server port - for remoet client connect
	true,			-- use authentication?
	'/path/to/passwd.file',			-- password file for authentication
	'/path/to/access.file',			-- access file for authentication
	true,			-- use SSL?
	'/path/to/keystore',			-- SSL keystore path
	'keystorepass',			-- SSL keystore password
	'null',			-- SSL trust store
	'null');		-- SSL trust store password
```

**启用具有身份验证和 SSL + SSL 客户端验证的 JMX MBean**

```sql
SELECT synchdb_add_jmx_conninfo(
	'mysqlconn',	-- existing connector name
	'0.0.0.0',		-- JMX listen address
	9010,			-- JMX listen port
	'10.55.13.17',	-- JMX RMI server address - for remote client connect
	9010,			-- JMX RMI server port - for remoet client connect
	true,			-- use authentication?
	'/path/to/passwd.file',			-- password file for authentication
	'/path/to/access.file',			-- access file for authentication
	true,			-- use SSL?
	'/path/to/keystore',			-- SSL keystore path
	'keystorepass',			-- SSL keystore password
	'/path/to/truststore',			-- SSL trust store
	'truststorepass');		-- SSL trust store password
```

## **使用 jconsole 可视化 JMX 指标**

当启动一个配置了 JMX 的连接器时，JMX 服务将在指定的端口号上运行。我们可以使用 Java 发行版自带的 jconsole 连接到 JMX 服务器。可以通过 JVM（连接器工作进程）PID 或 IP 地址和端口号进行本地连接。如果启用了身份验证，则还需要用户名和密码。如果不使用身份验证，则这些可以留空。

![img](../../images/jmx.jpg)

连接成功后，我们可以查看 JVM 运行指标的所有详细信息，例如 CPU、内存、类利用率、线程数等。

![img](../../images/jmx2.jpg)
![img](../../images/jmx3.jpg)
![img](../../images/jmx4.jpg)

## **可视化 Debezium JMX MBean**

最后一个选项卡是 MBean，其中包含 Debezium 针对架构历史记录、快照和流式传输阶段的特定指标。

![img](../../images/jmx5.jpg)
![img](../../images/jmx6.jpg)
![img](../../images/jmx7.jpg)

根据连接器类型，MBean 指标可能会有所不同。有关捕获指标列表，请参阅 Debezium 连接器文档：

* [MySQL 的 MBean](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-snapshot-metrics)
* [SQL Server 的 MBean](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html#sqlserver-snapshot-metrics)
* [Oracle 的 MBean](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-snapshot-metrics)