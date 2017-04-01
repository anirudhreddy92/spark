package com.learning1;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMapExample1 {

	public static void main(String[] args) {
		String localFilePath = "/home/cloudera/cm_api.py";
		String hdfsFilePath = "hdfs://quickstart:8020/user/cloudera/categories/";
		SparkConf conf = new SparkConf().setAppName("App 1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> logData = sc.textFile(localFilePath).cache();
		JavaRDD<String> logData = sc.textFile(hdfsFilePath).cache();

		class FlatMap implements FlatMapFunction<String, String> {

			@Override
			public Iterator<String> call(String line) throws Exception {

				return Arrays.asList(line.toUpperCase().split(",")[2]).iterator();
			}

		}

		JavaRDD<String> flatmapRDD = logData.flatMap(new FlatMap());
		System.out.println(flatmapRDD.take(10));

	}

}
