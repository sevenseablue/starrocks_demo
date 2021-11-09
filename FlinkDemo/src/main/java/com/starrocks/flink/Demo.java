package com.starrocks.flink; /**
 * @ClassName Demo
 * @Date 2021/9/29 20:35
 * @Author dq
 * @Description TODO
 */

import com.starrocks.connector.flink.table.StarRocksSinkSemantic;

public class Demo {
  public static void main(String[] args) {
    StarRocksSinkSemantic test = StarRocksSinkSemantic.fromName("test");
  }
}
