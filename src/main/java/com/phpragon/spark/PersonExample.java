package com.phpragon.spark;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @Description: Spark的一些基础操作示例
 * @author: phpdragon@qq.com
 * @date: 2020/03/30 17:21
 */
@Slf4j
public class PersonExample {

    private static String DATA_SOURCE_URL = "jdbc:mysql://localhost:3306/db_demo";

    private static String DB_TABLE_NAME = "t_person";

    private static String INPUT_TXT_PATH = "hdfs://172.16.1.126:9000/test/person.txt";

    private static String OUTPUT_PATH = "hdfs://172.16.1.126:9000/test/";

//    private static String INPUT_TXT_PATH = "test/person.txt";
//
//    private static String OUTPUT_PATH = "test/";

    public static void readPersonData(SparkSession spark) {
        // 通过一个文本文件创建Person对象的RDD
        JavaRDD<ReadHdfsFile.Person> peopleRDD = spark.read()
                .textFile(INPUT_TXT_PATH)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    ReadHdfsFile.Person person = new ReadHdfsFile.Person();
                    person.setId(Integer.parseInt(parts[0].trim()));
                    person.setName(parts[1].trim());
                    person.setAge(Integer.parseInt(parts[2].trim()));
                    return person;
                });

        // 对JavaBeans的RDD指定schema得到DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, ReadHdfsFile.Person.class);

        //输出数据结构
        peopleDF.show();
        /**
         * +---+---+--------+
         * |age| id|    name|
         * +---+---+--------+
         * | 20|  1|zhangsan|
         * | 29|  2|    lisi|
         * | 25|  3|  wangwu|
         * | 30|  4| zhaoliu|
         * | 35|  5|  tianqi|
         * | 40|  6|    kobe|
         * +---+---+--------+
         */

        // 注册该DataFrame为临时视图
        peopleDF.createOrReplaceTempView("people");
        // 执行SQL语句,获取到结果集Dataset
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 25 AND 35");
        teenagersDF.show();
        /**
         * +-------+
         * |   name|
         * +-------+
         * |   lisi|
         * | wangwu|
         * |zhaoliu|
         * | tianqi|
         * +-------+
         */

        // 结果中的列 通过属性的名字获取
        Encoder<String> stringEncoder2 = Encoders.STRING();
        // 或者通过属性的名字获取
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }, stringEncoder2);
        teenagerNamesByIndexDF.show();
        /**
         * +-------+
         * |  value|
         * +-------+
         * |   lisi|
         * | wangwu|
         * |zhaoliu|
         * | tianqi|
         * +-------+
         */


        // 结果中的列 通过属性的名字获取
        Encoder<String> stringEncoder = Encoders.STRING();
        // 或者通过属性的名字获取
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("name");
            }
        }, stringEncoder);
        teenagerNamesByFieldDF.show();
        /**
         * +-------+
         * |  value|
         * +-------+
         * |   lisi|
         * | wangwu|
         * |zhaoliu|
         * | tianqi|
         * +-------+
         */


        spark.close();
    }

    /**
     * 读取person.txt文件到MySQL中
     */
    public static void writePersonDataToMySql(SparkSession spark, Properties dbProperties) {
        //######################################################
        // 创建一个RDD
        JavaRDD<Row> personsRDD = spark.read()
                .textFile(INPUT_TXT_PATH)
                .toJavaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    return RowFactory.create(Integer.valueOf(parts[0]), parts[1], Integer.valueOf(parts[2]));
                });
        // 手动定义schema 生成StructType
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        //构建StructType，用于最后DataFrame元数据的描述
        StructType schema = DataTypes.createStructType(fields);
        //基于已有的元数据以及RDD<Row>来构造DataFrame
        Dataset<Row> personsDataFrame = spark.sqlContext().createDataFrame(personsRDD, schema);

        //上部分代码等同下面的代码，支持反射获取Schema
        //目前的Spark SQL版本不支持包含Map field(s)的JavaBeans，但嵌套的JavaBeans和List或者Array fields是支持的。
        //可以通过创建一个实现Serializable接口和包含所有fields的getters和setters方法的类来创建一个JavaBean。

        //对JavaBeans的RDD指定schema得到DataFrame
        JavaRDD<ReadHdfsFile.Person> personsRDD_2 = spark.read()
                .textFile("spark-warehouse/person.txt")
                .toJavaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    ReadHdfsFile.Person person = new ReadHdfsFile.Person();
                    person.setId(Integer.parseInt(parts[0]));
                    person.setName(parts[1]);
                    person.setAge(Integer.parseInt(parts[2]));
                    return person;
                });
        personsDataFrame = spark.createDataFrame(personsRDD_2, ReadHdfsFile.Person.class);

        //######################################################

        // 使用DataFrame创建临时视图person
        personsDataFrame.createOrReplaceTempView("person");
        //将数据写入到MySql t_person表中
        personsDataFrame.write().mode("append").jdbc(DATA_SOURCE_URL, DB_TABLE_NAME, dbProperties);


        // 运行查询视图person
        // SQL查询的结果是DataFrames类型，支持所有一般的RDD操作
        // 结果的列可以通过属性的下标或者名字获取
        Dataset<String> results = spark.sql("SELECT name FROM person").map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("name");
            }
        }, Encoders.STRING());

        //控制台打印
        results.show();
    }

    /**
     * 读取数据
     *
     * @param spark
     */
    public static void readMysqlTableDataToTxt(SparkSession spark,Properties dbProperties) {
        log.info("读取db_bigdata数据库中的t_person表内容");

        //读取表中所有数据到临时视图person
        spark.read().jdbc(DATA_SOURCE_URL, DB_TABLE_NAME, dbProperties).createOrReplaceTempView("person");
        Dataset<Row> persons = spark.sql("SELECT * FROM person");

        //输出结果文件的hdfs路径
        String outputPath = OUTPUT_PATH + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

        persons.write().save(outputPath);
    }

    @Data
    public static class Person implements Serializable {
        private int id;
        private String name;
        private int age;
    }
}
