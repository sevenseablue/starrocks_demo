package com.starrocks.flink;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.flink.bean.TableData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.util.Random;

/**
 * @author StarRocks
 * @date 2021/8/19 14:14
 * @desc
 */
public class BeanDataTest {

  private static Random random = new Random();

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
    //对数据做ETL，将数据封装成Bean
    SingleOutputStreamOperator<TableData> dataStreamSource = streamSource.map(new MapFunction<String, TableData>() {
      @Override
      public TableData map(String value) throws Exception {
        System.out.println("receive data:" + value);
        int siteId = random.nextInt(10000);
        int cityCode = random.nextInt(1000);
        return new TableData(siteId, cityCode, value, 1);
      }
    });
    //将Bean类型的数据sink到starrocks
    dataStreamSource.addSink(StarRocksSink.sink(
        // the table structure
        TableSchema.builder()
            .field("siteid", DataTypes.INT())
            .field("citycode", DataTypes.INT())
            .field("username", DataTypes.VARCHAR(32))
            .field("pv", DataTypes.INT())
            .build(),
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("connector", "starrocks")
            .withProperty("jdbc-url", "jdbc:mysql://localhost:9030?characterEncoding=utf-8&useSSL=false")
            .withProperty("load-url", "localhost:18030")
            .withProperty("username", "test")
            .withProperty("password", "123456")
            .withProperty("table-name", "table1")
            .withProperty("database-name", "example_db")
            //设置列分隔符
            .withProperty("sink.properties.column_separator", "\\x01")
            //设置行分隔符
            .withProperty("sink.properties.row_delimiter", "\\x02")
            //设置sink提交周期，这里设置10s提交一次
            .withProperty("sink.buffer-flush.interval-ms", "1000")
            .build(),
        // set the slots with streamRowData
        (slots, streamRowData) -> {
          slots[0] = streamRowData.getSiteid();
          slots[1] = streamRowData.getCitycode();
          slots[2] = streamRowData.getUsername();
          slots[3] = streamRowData.getPv();
        }
    ));

    env.execute();
  }
}
