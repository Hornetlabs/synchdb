# OLR 旧位点恢复回归测试

使用独立临时 PostgreSQL 集群，不连接 Oracle/OLR，不修改已安装扩展或用户实例。

```sh
SYNCHDB_TARGET_BIN=/path/to/postgresql/bin \
  venv/bin/python -m pytest -q src/test/pytests/olr_legacy
```

需要本机 OLR 构建依赖。测试模块只在指定临时目录生成，不会安装或新增生产 SQL 接口。
它直接编译生产 `olr_client.c` 与 `olr_handoff.c`，覆盖旧位点落在事务中间的首段恢复、同 SCN 后续索引、零索引、普通快照边界和数据/位点事务回滚。

测试将目录定位重定向到临时 `fixture.positions` 表，将行写入替换为简单 INSERT；位点 UPDATE、事件过滤和帧边界判断使用生产实现。这些测试验证恢复机制，不代替真实旧版到新版的 Oracle/OLR 升级验证。
