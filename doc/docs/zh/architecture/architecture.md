# 架构概述

## **总体架构图**

![img](../../images/synchdb-arch2.jpg)

SynchDB 扩展包含两个代码空间（Java 和 C），JNI 位于两者之间作为协调员。

**Debezium Runner - Java**

* Debezium Runner 驱动程序
* 嵌入式 Debezium 引擎

**SynchDB 扩展 - C**

* SynchDB 启动器
* SynchDB Worker
* 格式转换器
* 复制代理
* 表同步代理（待定）

这两个代码空间通过 Java 原生接口 (JNI) 互连。JNI 是一个框架，允许 Java 应用程序与用 C 或 C++ 等语言编写的原生代码交互，反之亦然。它使 Java 程序能够调用原生应用程序和库，并被原生应用程序和库调用，从而在 Java 的平台独立性和原生代码的性能优势之间架起了一座桥梁。JNI 通常用于集成特定于平台的功能、优化应用程序中性能关键部分，或访问 Java 中不可用的旧库。它需要谨慎管理资源，因为它涉及在 Java 虚拟机 (JVM) 和本机环境之间切换，这可能会增加复杂性。

因此，SynchDB 需要 JNI 在 Debezium 运行器引擎和 SynchDB PostgreSQL 扩展之间交换资源。JNI 可在 Java 安装（例如 openjdk）中使用。

### **Debezium Runner - Java**

这是一个 Java 驱动程序，它利用 Debezium 嵌入式引擎以及各种连接到不同数据库源的“连接器”。它是实现从多个数据库供应商进行逻辑复制的关键底层组件。它的主要职责是连接到指定的远程数据库，并定期获取其更改并将其转换为通用的 JSON 结构。然后，此 JSON 结构将传递给“C 语言 SynchDB 扩展”进行处理，并最终将更改应用于 PostgreSQL。

[Debezium Runner 组件架构](../../architecture/debezium_runner_components/)

### **SynchDB 扩展 - C**

这是初始化 Java 虚拟机 (JVM) 并在其上运行 Debezium Runner 的主要入口点。它会定期从 Debezium Runner 获取一批 JSON 变更事件，处理这些数据并将其应用到 PostgreSQL。它还负责通知 Debezium 已成功完成一批 JSON 变更事件，以便两个组件在复制进度方面保持同步。

* [Debezium 事件处理器架构](../../architecture/debezium_event_processor/)
* [Openlog Replicator 事件处理器架构](../../architecture/openlog_replicator_event_processor/)