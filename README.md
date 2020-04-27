# spark-example
>Spark应用开发示例代码

### 环境搭建
- Hadoop：[CentOS7 部署 Hadoop 3.2.1 (伪分布式)](https://www.cnblogs.com/phpdragon/p/12592572.html)
- Spark：[CentOS7 安装 Spark3.0.0-preview2-bin-hadoop3.2](https://www.cnblogs.com/phpdragon/p/12594866.html)

### 分词统计
```bash
#新建输入输出目录
hdfs dfs -mkdir /input
hdfs dfs -mkdir /output
hdfs dfs -mkdir /spark
hdfs dfs -mkdir /spark/history
```
```bash
#上传测试文件
hdfs dfs -put ~/data/server/hadoop/3.2.1/LICENSE.txt /input/test.txt

# 命令的最后三个参数，是java的main方法的入参，具体的使用请参照WordCount类的源码
/data/server/spark/3.0.0-preview2-bin-hadoop3.2/bin/spark-submit \
--master spark://172.16.1.126:7077 \
--class com.phpragon.spark.WordCount \
--executor-memory 512m \
--total-executor-cores 2 \
./spark-example-1.0-SNAPSHOT.jar \
172.16.1.126 \
9000 \
test.txt
```

### 读取hdfs上的文件并输出
> vi person.txt
```xml
1,zhangsan,20
2,lisi,29
3,wangwu,25
4,zhaoliu,30
5,tianqi,35
6,kobe,40
```
```bash
#上传测试文件
hdfs dfs -put ./person.txt /test/person.txt

# 命令的最后三个参数，是java的main方法的入参，具体的使用请参照WordCount类的源码
/data/server/spark/3.0.0-preview2-bin-hadoop3.2/bin/spark-submit \
--master spark://172.16.1.126:7077 \
--class com.phpragon.spark.ReadHdfsFile \
--executor-memory 512m \
--total-executor-cores 2 \
./spark-example-1.0-SNAPSHOT.jar
```

### 读取hdfs上的文件并写入到MySql
```bash
#上传测试文件
hdfs dfs -put ./person.txt /test/person.txt

# 命令的最后三个参数，是java的main方法的入参，具体的使用请参照WordCount类的源码
/data/server/spark/3.0.0-preview2-bin-hadoop3.2/bin/spark-submit \
--master spark://172.16.1.126:7077 \
--class com.phpragon.spark.ReadHdfsFileWriteToMySql \
--executor-memory 512m \
--total-executor-cores 2 \
./spark-example-1.0-SNAPSHOT.jar
```


### 读取MySql并写入到hdfs文件
```bash
#上传测试文件
hdfs dfs -put ./person.txt /test/person.txt

# 命令的最后三个参数，是java的main方法的入参，具体的使用请参照WordCount类的源码
/data/server/spark/3.0.0-preview2-bin-hadoop3.2/bin/spark-submit \
--master spark://172.16.1.126:7077 \
--class com.phpragon.spark.ReadMySqlDataWriteToHdfsFile \
--executor-memory 512m \
--total-executor-cores 2 \
./spark-example-1.0-SNAPSHOT.jar
```
