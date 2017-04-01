package com.learning1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class MapExample1 {

	public static void main(String[] args) {

		String localFilePath = "/home/cloudera/cm_api.py";
		String hdfsFilePath  = "hdfs://quickstart:8020/user/cloudera/categories/";
		SparkConf conf = new SparkConf().setAppName("App 1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> logData = sc.textFile(localFilePath).cache();
		JavaRDD<String> logData = sc.textFile(hdfsFilePath).cache();

		class MapFunction implements Function<String, String> {
			String condition;		
			
			public MapFunction(String condition) {
				this.condition = condition;
			}
			public String call(String v1) throws Exception {
				if("lower".equals(condition))
					return v1.toLowerCase();
				
				return v1.toUpperCase();
			}
		}
		
		JavaRDD<String> uppercase = logData.map(new MapFunction("lower"));
		System.out.println(uppercase.take(5));
		
		JavaRDD<String> lowercase = logData.map(new MapFunction(""));
		System.out.println(lowercase.take(5));

		

	}
}
