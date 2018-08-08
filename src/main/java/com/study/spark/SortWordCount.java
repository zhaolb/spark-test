package com.study.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author Mtime
 * @date 2018/8/7.
 */
public class SortWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaRDD<String> lines;
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            lines = sc.textFile("D:\\spark.txt");
            JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
            JavaPairRDD<String, Integer> wordPair = words.mapToPair(word -> new Tuple2<>(word, 1));
            JavaPairRDD<String, Integer> wordCount = wordPair.reduceByKey((a, b) -> (a + b));
            JavaPairRDD<Integer, String> countWord = wordCount.mapToPair(word -> new Tuple2<>(word._2, word._1));
            JavaPairRDD<Integer, String> sortedCountWord = countWord.sortByKey(false);
            JavaPairRDD<String, Integer> sortedWordCount = sortedCountWord.mapToPair(word -> new Tuple2<>(word._2, word._1));
            sortedWordCount.foreach(s -> System.out.println("word " + s._1 + " appears " + s._2 + " times"));
        }
    }
}
