package com.learning1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReduceByKeyExample1 {
	public static void main(String[] args) {
		String localFilePath = "/home/cloudera/cm_api.py";
		String hdfsFilePath = "hdfs://quickstart:8020/user/cloudera/categories/part-m-00000";
		SparkConf conf = new SparkConf().setAppName("App 1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> logData = sc.textFile(localFilePath).cache();
		JavaRDD<String> logData = sc.textFile(hdfsFilePath).cache();

		class PairRdd implements PairFunction<String, String, Integer> {

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {

				return new Tuple2(arg0.split(",")[2], 1);
			}

		}

		class ReducedRdd implements Function2<Integer, Integer, Integer> {

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}

		}

		JavaPairRDD<String, Integer> pairRdd = logData.mapToPair(new PairRdd());

		JavaPairRDD<String, Integer> reducedRdd = pairRdd.reduceByKey(new ReducedRdd());
		System.out.println(reducedRdd.take(10));

	}
}
