package com.starrocks.flink;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

/**
 * @author StarRocks
 * @date 2021/8/19 14:14
 * @desc
 */
public class JsonDataTest {

  private static Random random = new Random();
  private static JSONObject jsonData = new JSONObject();

  public static void main(String[] args) throws Exception {
    //初始化环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //设置并行度
    env.setParallelism(2);

    /**
     * 动态数据源，从9999端口接收数据
     * 命令：nc -lk 9999
     */
    DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
    //对数据做ETL，将数据封装成JSON格式
    SingleOutputStreamOperator<String> dataStreamSource = streamSource.map(new MapFunction<String, String>() {
      @Override
      public String map(String value) throws Exception {
        System.out.println("receive data:" + value);
        int siteId = random.nextInt(10000);
        int cityCode = random.nextInt(1000);
        jsonData.put("siteid", siteId);
        jsonData.put("citycode", cityCode);
        jsonData.put("username", value);
        jsonData.put("pv", 1);
        return jsonData.toJSONString();
      }
    });
    //将JSON类型的数据sink到starrocks
    dataStreamSource.addSink(StarRocksSink.sink(
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("connector", "starrocks")
            .withProperty("jdbc-url", "jdbc:mysql://localhost:9030?characterEncoding=utf-8&useSSL=false")
            .withProperty("load-url", "localhost:18030")
            .withProperty("username", "test")
            .withProperty("password", "123456")
            .withProperty("table-name", "table1")
            .withProperty("database-name", "example_db")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .withProperty("sink.buffer-flush.interval-ms", "10000")
            .build()
    ));

    env.execute();
  }
}
