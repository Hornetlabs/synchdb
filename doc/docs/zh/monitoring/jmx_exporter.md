# JMX Exporter

## 什么是 JMX Exporter

JMX Exporter（也称为 JMX Prometheus Java Agent）是一个 Java 代理，它以 Prometheus 可以抓取和监控的格式公开 Java 应用程序的 JMX 指标。它是由 Prometheus 社区创建的一款工具，支持以下功能：

* 访问 JVM 内部指标（例如内存使用情况、线程数、GC 统计信息等）
* 通过 MBean 公开自定义应用程序指标
* 通过 HTTP 端点导出这些指标（例如 http://localhost:9404/metrics）
* 将 Java 应用程序（例如 Kafka、Cassandra 或您自己的应用程序）集成到 Prometheus 监控设置中

此工具可解锁基于 Prometheus + Graphana 的 SynchDB 连接器监控功能

![img](../../images/prom.png)

## 获取 JMX 导出器

预编译的 JMX 导出器 (.jar) 可在官方 [JMX 导出器发布页面](https://github.com/prometheus/jmx_exporter/releases) 获取。我们感兴趣的是“jmx_prometheus_javaagent”工具，例如：

```
jmx_prometheus_javaagent-1.3.0.jar

```

请将其下载到运行 SynchDB 连接器的同一台机器上。

## 编写 JMX 导出器配置文件

JMX 导出器需要一个配置文件来定义暴露指标的行为。以下是一个基本的配置模板，可帮助您入门。

```
startDelaySeconds: 0
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  - pattern: ".*"

```

有关更多高级配置参数及其使用方法，请参阅[官方 prometheus 文档](https://prometheus.github.io/jmx_exporter/1.3.0/http-mode/rules/)。

## 将 JMX 导出器配置到 SynchDB 连接器

synchdb_add_jmx_exporter_conninfo() 和 synchdb_del_jmx_exporter_conninfo() 函数用于向现有连接器添加或删除 JMX 导出器配置。这支持通过 Prometheus 和 Graphana 等工具进行运行时监控和诊断。

**函数签名**

```
synchdb_add_jmx_exporter_conninfo(
	name TEXT,
	exporter_jar_path TEXT,
	exporter_port INTEGER,
	config_file_path TEXT
);

synchdb_del_jmx_exporter_conninfo(
	name text
)

```

| Parameter           | Type   | Description                                                                 |
| ------------------- | ------ | --------------------------------------------------------------------------- |
| `connector_name`    | `TEXT` | Name of the existing connector you want to attach the JMX Exporter to.      |
| `exporter_jar_path` | `TEXT` | Absolute path to the `jmx_prometheus_javaagent.jar` file.                   |
| `exporter_port`     | `INT`  | Port on which the JMX Exporter HTTP server will expose metrics (e.g. 9404). |
| `config_file_path`  | `TEXT` | Path to the JMX Exporter's YAML configuration file we defined above.        |

```sql
SELECT synchdb_add_jmx_exporter_conninfo(
	'mysqlconn',	-- existing connector name
	'/path/to/jmx_exporter/jar',		-- path to JMX exporter java agent jar
	9404,			-- JMX exporter running port
	'/path/to/jmx/conf');		-- path to JMX exporter conf file

```

## 通过 HTTP 获取指标

当连接器使用 JMX 导出器设置启动时，它将在以下位置公开指标：

```
http://<host>:9404/metrics
```

我们可以通过以下方式测试：
```
curl http://<host>:9404/metrics

# HELP debezium_mysql_connector_metrics_binlogposition debezium.mysql:name=null,type=connector-metrics,attribute=BinlogPosition
# TYPE debezium_mysql_connector_metrics_binlogposition untyped
debezium_mysql_connector_metrics_binlogposition{context="streaming",server="synchdb-connector"} 1500.0
# HELP debezium_mysql_connector_metrics_changesapplied debezium.mysql:name=null,type=connector-metrics,attribute=ChangesApplied
# TYPE debezium_mysql_connector_metrics_changesapplied untyped
debezium_mysql_connector_metrics_changesapplied{context="schema-history",server="synchdb-connector"} 39.0
# HELP debezium_mysql_connector_metrics_changesrecovered debezium.mysql:name=null,type=connector-metrics,attribute=ChangesRecovered
# TYPE debezium_mysql_connector_metrics_changesrecovered untyped
debezium_mysql_connector_metrics_changesrecovered{context="schema-history",server="synchdb-connector"} 26.0
# HELP debezium_mysql_connector_metrics_connected debezium.mysql:name=null,type=connector-metrics,attribute=Connected
# TYPE debezium_mysql_connector_metrics_connected untyped
debezium_mysql_connector_metrics_connected{context="streaming",server="synchdb-connector"} 1.0

...
...
...
```

## Prometheus 和 Graphana

一旦我们确认可以通过 HTTP 获取指标，就可以将此端点配置到 Prometheus 系统，并让其“抓取”所有指标。然后，我们可以在 graphana 中创建一个 Prometheus 数据源，并使用它创建一个仪表板。请参阅 Prometheus 和 graphana [教程](https://grafana.com/docs/grafana/latest/getting-started/get-started-grafana-prometheus/) 了解如何操作。

为了快速测试，您还可以使用 `ezdeploy` 工具快速部署 prometheus 和 grafana，并使用 Synchdb 内置的仪表板模板。有关模式详细信息，请参阅 [快速入门指南](../../getting-started/quick_start/)。