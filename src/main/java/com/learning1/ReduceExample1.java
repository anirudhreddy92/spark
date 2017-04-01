package com.learning1;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ReduceExample1 {

	public static void main(String[] args) {
		String localFilePath = "/home/cloudera/cm_api.py";
		String hdfsFilePath = "hdfs://quickstart:8020/user/cloudera/categories/part-m-00000";
		SparkConf conf = new SparkConf().setAppName("App 1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> logData = sc.textFile(localFilePath).cache();
		JavaRDD<String> logData = sc.textFile(hdfsFilePath).cache();
		
		class ReduceFunctionExample implements Function2<String, String, String>{

			@Override
			public String call(String arg0, String arg1) throws Exception {
				
				return  arg0.split(",")[1].concat(",") +  arg1.split(",")[2];
			}
			
		}
		
		
		
		String reducedRDD = logData.reduce(new ReduceFunctionExample());
		System.out.println(reducedRDD);
		
		JavaRDD<Integer> listData = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));
		
//		listData.reduce(new ReduceFunctionExample());


	}

}
