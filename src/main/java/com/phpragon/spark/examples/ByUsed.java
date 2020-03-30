package com.phpragon.spark.examples;

import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * @author Carl
 * @title: ByUsed
 * @projectName BigdataSystem
 * @description: TODO
 * @date 2019/8/2 10:31
 */
public class ByUsed {

    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("" +
                "SELECT\n" +
                "o1.cust_id,                                             --用戶id\n" +
                "o1.free_prd_id,                                         --商品id\n" +
                "greatest(o3.score, o4.score) as score,                  --评分\n" +
                "912580553  as other,                                    --其他值\n" +
                "date_add('2019-08-04',-0)   as  stat_date,                 --統計日期\n" +
                "from_unixtime(unix_timestamp()) as update_time          --更新日期      \n" +
                "FROM\n" +
                "  (SELECT\n" +
                "      aa.cust_id,\n" +
                "      aa.free_prd_id\n" +
                "    FROM\n" +
                "      (SELECT\n" +
                "          a.free_prd_id,\n" +
                "          a.cust_id\n" +
                "        FROM db_bi.t_free_order a\n" +
                "        LEFT JOIN db_bi.t_order_detail b ON a.ord_id = b.ord_id\n" +
                "        WHERE b.ord_status = 2\n" +
                "        GROUP BY a.free_prd_id,a.cust_id\n" +
                "      ) aa\n" +
                "    UNION\n" +
                "      SELECT\n" +
                "        cust_id,\n" +
                "        free_prd_id\n" +
                "      FROM db_bi.t_cust_freeprd_detailpage_pv\n" +
                "      WHERE dt = '2090804'\n" +
                "      GROUP BY cust_id,free_prd_id\n" +
                "  ) o1\n" +
                "LEFT JOIN\n" +
                "(SELECT\n" +
                "    cust_id,\n" +
                "    free_prd_id,\n" +
                "    2 score\n" +
                "  FROM db_bi.t_cust_freeprd_detailpage_pv\n" +
                "  WHERE dt = '20190804'\n" +
                "  GROUP BY cust_id,free_prd_id\n" +
                ") o3 ON o1.cust_id = o3.cust_id\n" +
                "AND o1.free_prd_id = o3.free_prd_id\n" +
                "LEFT JOIN\n" +
                "(SELECT\n" +
                "    a.free_prd_id,\n" +
                "    a.cust_id,\n" +
                "    5 score\n" +
                "  FROM db_bi.t_free_order a\n" +
                "  LEFT JOIN db_bi.t_order_detail b ON a.ord_id = b.ord_id\n" +
                "  WHERE b.ord_status = 2\n" +
                "  GROUP BY a.free_prd_id,a.cust_id\n" +
                ") o4 ON o1.cust_id = o4.cust_id AND o1.free_prd_id = o4.free_prd_id\n" +
                "LEFT JOIN db_ods.t_prd_merchandise_status o5 ON o1.free_prd_id = o5.free_prd_id\n" +
                "WHERE o5.STATUS = 1");

           }

}
