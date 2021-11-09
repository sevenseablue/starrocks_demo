package com.starrocks.flink.bean;

/**
 * 建库建表语句：
 * create database example_db;
 * <p>
 * use example_db;
 * <p>
 * CREATE TABLE table1
 * (
 * siteid INT DEFAULT '10',
 * citycode SMALLINT,
 * username VARCHAR(32) DEFAULT '',
 * pv BIGINT SUM DEFAULT '0'
 * )
 * AGGREGATE KEY(siteid, citycode, username)
 * DISTRIBUTED BY HASH(siteid) BUCKETS 10
 * PROPERTIES("replication_num" = "3");
 */

public class TableData {
  /**
   * siteid INT DEFAULT '10',
   * citycode SMALLINT,
   * username VARCHAR(32) DEFAULT '',
   * pv BIGINT SUM DEFAULT '0'
   */
  private int siteid;
  private int citycode;
  private String username;
  private int pv;

  public TableData(int siteid, int citycode, String username, int pv) {
    this.siteid = siteid;
    this.citycode = citycode;
    this.username = username;
    this.pv = pv;
  }

  public int getSiteid() {
    return siteid;
  }

  public void setSiteid(int siteid) {
    this.siteid = siteid;
  }

  public int getCitycode() {
    return citycode;
  }

  public void setCitycode(int citycode) {
    this.citycode = citycode;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public int getPv() {
    return pv;
  }

  public void setPv(int pv) {
    this.pv = pv;
  }
}
