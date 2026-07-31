---
weight: 10
---
# 安装

## **从软件包安装**
访问我们的[发布页面](https://github.com/Hornetlabs/synchdb/releases) 下载 synchdb 软件包。

### **.deb 包**

1. 从官方 apt 存储库安装 PostgreSQL（版本 16 为例）：
```sh linenums="1"
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install postgresql-16
```

2. 安装 Java 运行时环境：
```sh linenums="1"
sudo apt install openjdk-17-jre-headless
```

3. 更新共享库路径：
```sh linenums="1"
JAVA_PATH=$(which java)
JRE_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JRE_LIB_PATH=${JRE_HOME_PATH}/lib
echo "$JRE_LIB_PATH" | sudo tee /etc/ld.so.conf.d/java.conf
sudo ldconfig
```

4. 安装 SynchDB：
```sh linenums="1"
dpkg -i synchdb-1.1-1.ub22.pg16_x86_64.deb
```
5. SynchDB 应该已准备就绪。请参阅 [快速入门](../../user-guide/quick_start/) 页面开始使用

### **.rpm 软件包**

1. 从官方 rpm 仓库安装 PostgreSQL（版本 16 为例）：
其他 PostgreSQL 版本的官方 rpm 下载说明，请参考此处 (https://www.postgresql.org/download/linux/redhat/)
```sh linenums="1"
sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo dnf -qy module disable postgresql
sudo dnf install -y postgresql16-server postgresql16-contrib
```
2. 安装 Java 运行环境：
```sh linenums="1"
sudo dnf install -y java-17-openjdk
```

3. 更新共享库路径：
```sh linenums="1"
JAVA_PATH=$(which java)
JRE_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JRE_LIB_PATH=${JRE_HOME_PATH}/lib
echo "$JRE_LIB_PATH" | sudo tee /etc/ld.so.conf.d/java.conf
sudo ldconfig
```

4. 安装 SynchDB：
```sh linenums="1"
sudo dnf install -y synchdb-1.1-1.el9.pg16.x86_64.rpm
```
5. SynchDB 应该已准备就绪。请参阅 [快速入门](../../user-guide/quick_start/) 页面开始使用

## **从预编译的二进制文件安装**

访问我们的[发布页面](https://github.com/Hornetlabs/synchdb/releases) 下载预编译二进制文件。我们目前支持基于 Debian 的 Linux 系统（如 Ubuntu）系统上的预编译二进制文件。其他平台将在不久的将来得到支持。SynchDB 预编译二进制文件需要先安装 PostgreSQL。它所需的 PostgreSQL 版本在包名称中描述。例如，`synchdb-1.1-1.ub22.pg16_x86_64.tar.gz` 是在 Ubuntu 22.04 上针对 PostgreSQL 16 编译的二进制文件。

### **预编译二进制文件**

1. 解压预编译二进制文件的 tar.gz 包：
```sh linenums="1"
tar xzvf synchdb-1.1-1.ub22.pg16_x86_64.tar.gz -C /tmp
```

2. 找出当前 PostgreSQL 的 lib 和 share 目录
```sh linenums="1"
LIBDIR=$(pg_config | grep -w LIBDIR | awk -F ' = ' '/LIBDIR/ {print $2}')
SHAREDIR=$(pg_config | grep -w SHAREDIR | awk -F ' = ' '/SHAREDIR/ {print $2}')
```

3. 将预编译的二进制文件复制到相应目录：
```sh linenums="1"
cp /tmp/synchdb-1.1-1.ub22.pg16_x86_64/usr/lib/postgresql/16/lib/* $LIBDIR
cp /tmp/synchdb-1.1-1.ub22.pg16_x86_64/usr/share/postgresql/16/extension/* $SHAREDIR
```

4. 安装 Java Runtime Environment：
```sh linenums="1"
sudo apt install openjdk-17-jre-headless
```

5. 更新共享库路径：
```sh linenums="1"
JAVA_PATH=$(which java)
JRE_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JRE_LIB_PATH=${JRE_HOME_PATH}/lib
echo "$JRE_LIB_PATH" | sudo tee /etc/ld.so.conf.d/java.conf
sudo ldconfig
```

6. SynchDB 应该已准备就绪。请参阅 [快速入门](../../user-guide/quick_start/) 页面开始使用

## **源代码编译及安装**

此选项要求您从源代码构建和安装 PostgreSQL 和 SynchDB。

### **系统要求**

构建和运行SynchDB需要以下软件。列出的版本是开发过程中测试过的版本。较旧的版本可能仍然可用。

* Java Development Kit 22. 下载地址：[点击这里](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.9.8. 下载地址：[点击这里](https://maven.apache.org/download.cgi)
* PostgreSQL 源代码. Git克隆地址：[点击这里](https://github.com/postgres/postgres). PostgreSQL编译要求请参考此[wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code)
* Docker compose 2.28.1 (用于测试). 参考[安装指南](https://docs.docker.com/compose/install/linux/)
* 基于Unix的操作系统，如Ubuntu 22.04或MacOS

**Openlog Replicator 连接器支持的额外要求（如果在构建过程中启用）**

* libprotobuf-c v1.5.2。请参阅[此处](https://github.com/protobuf-c/protobuf-c.git) 从源代码构建。

**如果您想使用基於 FDW 的快照，則需要以下內容：**

* OCI v23.9.0。更多資訊請參閱[此處](../../user-guide/configure_snapshot_engine/)。
* oracle_fdw v2.8.0。如何從原始碼建置？請參閱[此處](https://github.com/laurenz/oracle_fdw)。

### **預設 SynchDB 建置 - 支援 MySQL、SQL Server 和 Oracle 連接器**

如果您已安裝 PostgreSQL，則可以使用 PGXS 建置並安裝預設 SynchDB。請注意，您的 PostgreSQL 安裝必須包含 SynchDB 所需的 pgcrypto 插件。

```sh
USE_PGXS=1 make PG_CONFIG=$(which pg_config)
USE_PGXS=1 make build_dbz PG_CONFIG=$(which pg_config)

sudo USE_PGXS=1 make PG_CONFIG=$(which pg_config) install
sudo USE_PGXS=1 make install_dbz PG_CONFIG=$(which pg_config)

```

### **建構支援 Openlog Replicator 連接器的 SynchDB**

要建置支援 Openlog Replicator 連接器的 SynchDB，還需要建置一個額外的 SynchDB Oracle 解析器元件。此元件基於 IvorySQL 的 Oracle 解析器，並針對 SynchDB 進行了修改，需要 PostgreSQL 後端原始程式碼才能成功建置。以下是具體步驟：

#### **准备源代码（以16.3为例）**

複製 PostgreSQL 原始碼並切換到 16.3 版本標籤

```sh linenums="1"
git clone https://github.com/postgres/postgres.git
cd postgres
git checkout REL_16_3

```

從擴充資料夾內克隆 SynchDB 原始碼

注意：目前使用分支 *(synchdb-devel)[https://github.com/Hornetlabs/synchdb/tree/synchdb-devel]* 進行開發。

```sh linenums="1"
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git

```

#### **编译和安装PostgreSQL**

您可以按照[此處](https://www.postgresql.org/docs/current/install-make.html)中所述的標準建置和安裝流程完成此操作。

**警告**：SynchDB 依賴 pgcrypto 來加密和解密敏感的存取資訊。請確保 PostgreSQL 已建置並啟用 SSL 支援。

```sh linenums="1"
cd /home/$USER/postgres
./configure --with-ssl=openssl --enable-cassert
make
sudo make install
```

建置所需的 pgcrypto 擴展

```sh linenums="1"
cd /home/$USER/postgres/contrib/pgcrypto
make
sudo make install

```

#### 建置 SynchDB 並新增 Openlog Replicator 連接器支持

```sh linenums="1"
# 建置並安裝 debezium runner
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
sudo make install_dbz

# 建置並安裝 Oracle 解析器
make oracle_parser
sudo make install_oracle_parser

# 建置並安裝 synchdb
make WITH_OLR=1
sudo make WITH_OLR=1 install

```

### 配置 Linker 以访问 Java（Ubuntu）
最后，我们还需要告诉系统的 Linker 新添加的 Java 库 (libjvm.so) 在系统中的位置。

``` BASH
# 动态设置 JDK 路径
JAVA_PATH=$(which java)
JDK_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JDK_LIB_PATH=${JDK_HOME_PATH}/lib

echo $JDK_LIB_PATH
echo $JDK_LIB_PATH/server

sudo echo "$JDK_LIB_PATH" ｜ sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
```
注意，对于搭载 M1/M2 芯片的 Mac，链接器文件位于 /etc/ld.so.conf.d/aarch64-linux-gnu.conf

运行 ldconfig 重新加载：
``` BASH
sudo ldconfig

```

确保 synchdo.so 扩展可以链接到系统上的 libjvm Java 库：
``` BASH
ldd synchdb.so
linux-vdso.so.1 (0x00007ffeae35a000)
libjvm.so => /usr/lib/jdk-22.0.1/lib/server/libjvm.so (0x00007fc1276c1000)
libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fc127498000)
libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007fc127493000)
libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fc12748e000)
librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007fc127489000)
libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fc1273a0000)
/lib64/ld-linux-x86-64.so.2 (0x00007fc128b81000)

```

如果 SynchDB 构建时支持 openlog replicator，请确保它可以链接到你系统上的 libprotobuf-c 库：
``` BASH
ldd synchdb.so
linux-vdso.so.1 (0x00007ffde6ba5000)
libjvm.so => /home/ubuntu/java/jdk-22.0.1/lib/server/libjvm.so (0x00007f3c8e191000)
libprotobuf-c.so.1 => /usr/local/lib/libprotobuf-c.so.1 (0x00007f3c8e186000)
libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f3c8df5d000)
libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007f3c8df58000)
libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007f3c8df53000)
librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007f3c8df4c000)
libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007f3c8de65000)
/lib64/ld-linux-x86-64.so.2 (0x00007f3c8f69e000)

```