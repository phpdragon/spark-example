package com.phpragon.spark.analysis;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 分析
 */
@Slf4j
public class NginxLogAnalysis {

    private static String INPUT_TXT_PATH;

    static {
        // /flume/nginx_logs/ 目录下的所有日志文件
        String datetime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        //TODO: 请设置你自己的服务器路径
        INPUT_TXT_PATH = "hdfs://172.16.1.126:9000/flume/nginx_logs/" + datetime + "/*.log";
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("NetworkWordCount(Java)")
                //TODO: 本地执行请启用这个设置
                //.master("local[*]")
                .getOrCreate();

        analysisNginxAllLog(spark);
        analysisNginx404Log(spark);
    }

    /**
     *
     * @param spark
     */
    private static void analysisNginx404Log(SparkSession spark) {
        // 通过一个文本文件创建Person对象的RDD
        JavaPairRDD<String, Integer> logsRDD = spark.read()
                .json(INPUT_TXT_PATH)
                .javaRDD()
                //.filter(row-> 404 == Long.parseLong(row.getAs("status").toString()))
                .filter(new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row row) throws Exception {
                        return 404 == Long.parseLong(row.getAs("status").toString());
                    }
                })
                .map(line -> {
                    return line.getAs("request_uri").toString();
                })
                //log是每一行数据的对象，value是1
                //.mapToPair(requestUri -> new Tuple2<>(requestUri, 1))
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String requestUri) throws Exception {
                        return new Tuple2<>(requestUri, 1);
                    }
                })
                //基于key进行reduce，逻辑是将value累加
                //.reduceByKey((value, lastValue) -> value + lastValue)
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer value, Integer lastValue) throws Exception {
                        return value + lastValue;
                    }
                });

        //先将key和value倒过来，再按照key排序
        JavaPairRDD<Integer, String> sorts = logsRDD
                //key和value颠倒，生成新的map
                .mapToPair(log -> new Tuple2<>(log._2(), log._1()))
                //按照key倒排序
                .sortByKey(false);


        //取前10个
//        FormatUtil.printJson(JSONObject.toJSONString(sorts.take(10)));

        // 手动定义schema 生成StructType
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("total(404)", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("request_uri", DataTypes.StringType, true));
        //构建StructType，用于最后DataFrame元数据的描述
        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rankingListRDD = sorts.map(log -> RowFactory.create(log._1(), log._2()));

        // 对JavaBeans的RDD指定schema得到DataFrame
        System.out.println("输出404状态的前10个URI：SELECT * FROM nginx_log_404 LIMIT 10");
        Dataset<Row> rankingListDF = spark.createDataFrame(rankingListRDD, schema);
        rankingListDF.createOrReplaceTempView("tv_nginx_log_404");
        rankingListDF = spark.sql("SELECT * FROM tv_nginx_log_404 LIMIT 10");
        rankingListDF.show();
    }

    private static void analysisNginxAllLog(SparkSession spark) {
        // 通过一个文本文件创建Person对象的RDD
        JavaPairRDD<String, Integer> logsRDD = spark.read()
                .json(INPUT_TXT_PATH)
                .javaRDD()
                .map(line -> line.getAs("request_uri").toString())
                //log是每一行数据的对象，value是1
                //.mapToPair(requestUri -> new Tuple2<>(requestUri, 1))
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String requestUri) throws Exception {
                        return new Tuple2<>(requestUri, 1);
                    }
                })
                //基于key进行reduce，逻辑是将value累加
                //.reduceByKey((value, lastValue) -> value + lastValue)
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer value, Integer lastValue) throws Exception {
                        return value + lastValue;
                    }
                });

        //先将key和value倒过来，再按照key排序
        JavaPairRDD<Integer, String> sorts = logsRDD
                //key和value颠倒，生成新的map
                .mapToPair(log -> new Tuple2<>(log._2(), log._1()))
                //按照key倒排序
                .sortByKey(false);

        //取前10个
        //System.out.println("取前10个：");
        //FormatUtil.printJson(JSONObject.toJSONString(sorts.take(10)));

        // 手动定义schema 生成StructType
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("total", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("request_uri", DataTypes.StringType, true));
        //构建StructType，用于最后DataFrame元数据的描述
        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rankingListRDD = sorts.map(log -> RowFactory.create(log._1(), log._2()));

        // 对JavaBeans的RDD指定schema得到DataFrame
        System.out.println("输出访问量前10的URI：SELECT * FROM tv_nginx_log LIMIT 10");
        Dataset<Row> rankingListDF = spark.createDataFrame(rankingListRDD, schema);
        rankingListDF.createOrReplaceTempView("tv_nginx_log");
        rankingListDF = spark.sql("SELECT * FROM tv_nginx_log LIMIT 10");
        rankingListDF.show();
    }

    public static void readNginxLog(SparkSession spark) {
        // 通过一个文本文件创建Person对象的RDD
        JavaRDD<NginxLog> logsRDD = spark.read()
                .json(INPUT_TXT_PATH)
                .javaRDD()
                .map(line -> {
                    NginxLog person = new NginxLog();
                    person.setRemoteAddr(line.getAs("remote_addr"));
                    person.setHttpXForwardedFor(line.getAs("http_x_forwarded_for"));
                    person.setTimeLocal(line.getAs("time_local"));
                    person.setStatus(line.getAs("status"));
                    person.setBodyBytesSent(line.getAs("body_bytes_sent"));
                    person.setHttpUserAgent(line.getAs("http_user_agent"));
                    person.setHttpReferer(line.getAs("http_referer"));
                    person.setRequestMethod(line.getAs("request_method"));
                    person.setRequestTime(line.getAs("request_time"));
                    person.setRequestUri(line.getAs("request_uri"));
                    person.setServerProtocol(line.getAs("server_protocol"));
                    person.setRequestBody(line.getAs("request_body"));
                    person.setHttpToken(line.getAs("http_token"));
                    return person;
                });

        JavaPairRDD<String, Integer> logsRairRDD = logsRDD
                //log是每一行数据的对象，value是1
                //.mapToPair(log -> new Tuple2<>(log.getRequestUri(), 1))
                .mapToPair(new PairFunction<NginxLog, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(NginxLog nginxLog) throws Exception {
                        return new Tuple2<String, Integer>(nginxLog.getRequestUri(), 1);
                    }
                })
                //基于key进行reduce，逻辑是将value累加
                //.reduceByKey((value, lastValue) -> value + lastValue)
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer value, Integer lastValue) throws Exception {
                        return value + lastValue;
                    }
                }).sortByKey(false);


        //先将key和value倒过来，再按照key排序
        JavaPairRDD<Integer, String> rankingListRDD = logsRairRDD
                //key和value颠倒，生成新的map
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
                //按照key倒排序
                .sortByKey(false);

        //取前10个
        List<Tuple2<Integer, String>> top10 = rankingListRDD.take(10);

        System.out.println(JSONObject.toJSONString(top10));

        // 对JavaBeans的RDD指定schema得到DataFrame
        Dataset<Row> allLogsDF = spark.createDataFrame(logsRDD, NginxLog.class);
        allLogsDF.show();
    }

    @Data
    public static class NginxLog implements Serializable {
        private String remoteAddr;
        private String httpXForwardedFor;
        private String timeLocal;
        private long status;
        private long bodyBytesSent;
        private String httpUserAgent;
        private String httpReferer;
        private String requestMethod;
        private String requestTime;
        private String requestUri;
        private String serverProtocol;
        private String requestBody;
        private String httpToken;
    }
}
