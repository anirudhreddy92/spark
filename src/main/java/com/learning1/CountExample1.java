package com.learning1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class CountExample1 {

	public static void main(String[] args) {

		String localFilePath = "/home/cloudera/cm_api.py";
		String hdfsFilePath  = "hdfs://quickstart:8020/user/cloudera/categories/";
		SparkConf conf = new SparkConf().setAppName("App 1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> logData = sc.textFile(localFilePath).cache();
		JavaRDD<String> logData = sc.textFile(hdfsFilePath).cache();

//		Function<String, Boolean> f = new Function<String, Boolean>() {
//
//			public Boolean call(String v1) throws Exception {
//				return v1.contains("def");
//			}
//		};
		class FilterFunction implements Function<String, Boolean> {

			public Boolean call(String v1) throws Exception {
				
				return v1.contains("def");
			}
		}
//		Function<String, Boolean> f = new FilterFunction();
//		JavaRDD<String> Lines = logData.filter(f);
		
//		JavaRDD<String> Lines = logData.filter(new FilterFunction());
		
//		JavaRDD<String> Lines = logData.filter( new Function<String, Boolean>() {
//
//			public Boolean call(String v1) throws Exception {
//				
//				return v1.contains("def");
//			}
//		});
		
		
		JavaRDD<String> Lines = logData.filter(s -> s.contains("3"));
		System.out.println("Lines -> " + Lines.first());
		long LinesCount = Lines.count();
		System.out.println("LinesCount -> " + LinesCount);

	}
}
