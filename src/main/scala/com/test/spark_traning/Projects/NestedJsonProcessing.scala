package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, get_json_object}
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

object NestedJsonProcessing extends App {


  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext
  import spark.implicits._

  // define a case class
  case class DeviceData (id: Int, device: String)
  // create some sample data
  val eventsDS = Seq (
    (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
    (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
    (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
    (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
    (4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
    (5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
    (6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
    (7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
    (8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
    (9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
    (10,"""{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
    (11,"""{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
    (12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
    (13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
    (14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
    (15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
    (16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
    (17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
    (18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
    (19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }"""))
    .toDF("id", "device").as[DeviceData]


 // eventsDS.show()

  val jsonSchema = new StructType()
    .add("battery_level", LongType)
    .add("c02_level", LongType)
    .add("cca3",StringType)
    .add("cn", StringType)
    .add("device_id", LongType)
    .add("device_type", StringType)
    .add("signal", LongType)
    .add("ip", StringType)
    .add("temp", LongType)
    .add("timestamp", TimestampType)

  /**
    * parse internal json string using from json object
   * and flatern deviceId
    */

  val eventJsonDS =  eventsDS.withColumn("jsonData",from_json('device, jsonSchema)).drop('device)
    .withColumn("deviceId", $"jsonData.device_id")

  eventJsonDS.printSchema()
  eventJsonDS.show()


  /**
    * parse json using get_json_object
    *
    */

  val eventJsonDS2= eventsDS.select('id,
    get_json_object('device, "$.battery_level").alias("battery_level"),
    get_json_object('device, "$.device_type").alias("deviceType"))


  // eventJsonDS2.show()





}
