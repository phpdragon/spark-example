package com.phpragon.spark;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @Description: Spark的一些基础操作示例
 * @author: phpdragon@qq.com
 * @date: 2020/03/30 17:21
 */
@Slf4j
public class ReadHdfsFile extends PersonExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("NetworkWordCount(Java)")
                //.master("local[*]")
                //.master("spark://172.16.1.126:7077")
                .getOrCreate();

        //数据库内容
        Properties dbProperties = new Properties();
        dbProperties.put("user", "root");
        dbProperties.put("password", "root1234");
        dbProperties.put("driver", "com.mysql.jdbc.Driver");

        readPersonData(spark);
    }

}
