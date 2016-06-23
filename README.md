### Introduction
This is a tool to get data from Canal, and sync to hbase.

### Setup Canal Server
Get canal source code from [here](https://github.com/chaopengio/canal)

The original Canal have protobuf version bug.

Follow the [instruction](https://github.com/alibaba/canal/wiki/AdminGuide).

### Setup Canal Client

Modify the config file in _src/main/resources/${env}}.properties_, then do

```shell
mvn clean package -P${env}
```

### Start Server
```shell
/path/to/canal.deployer/bin/startup.sh
```

### Start Client
```shell
nohup java -jar canal2hbase-0.0.1-SNAPSHOT-jar-with-dependencies.jar &
```
