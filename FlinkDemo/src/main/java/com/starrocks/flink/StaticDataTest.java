package com.starrocks.flink;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.flink.bean.TableData;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.util.Arrays;

/**
 * @author StarRocks
 * @date 2021/8/19 14:14
 * @desc
 */
public class StaticDataTest {

  public static void main(String[] args) throws Exception {
    //初始化环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //设置并行度
    env.setParallelism(2);

    /**
     * 静态数据源
     */
    DataStreamSource<TableData> dataStreamSource = env.fromCollection(Arrays.asList(
        new TableData(1001, 9110, "张三", 1),
        new TableData(1002, 9111, "李四", 1),
        new TableData(1003, 9112, "王五", 1),
        new TableData(1004, 9113, "赵六", 1),
        new TableData(1005, 9114, "田七", 1)
//                new TableData(1006, 9115, "adsf", 1)
    ));

    //将数据sink到starrocks
    dataStreamSource.addSink(StarRocksSink.sink(
        // the table structure
        TableSchema.builder()
            .field("siteid", DataTypes.INT())
            .field("citycode", DataTypes.INT())
            .field("username", DataTypes.VARCHAR(10))
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
