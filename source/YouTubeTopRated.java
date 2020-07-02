// Package definition
package com.bdp.youtube;

// importing all the necessary Java and Hadoop libraries

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class YouTubeTopRated {

	// Extending the Mapper default class with keyIn as LongWritable , ValueIn as Text, KeyOut as Text and ValueOut as FloatWritable.
    public static class Map extends Mapper<LongWritable,Text,Text,FloatWritable>{
        private  FloatWritable rating_value = new FloatWritable(); // variable to store the rating
        private Text videoId = new Text(); // variable to store the videoId
	// overriding map that runs for every line of input
        public void map(LongWritable key, Text value,
                        Context context) throws IOException,InterruptedException {
	    // Storing the each line and converting to string
            String row = value.toString();
	    // Splitting each record on tab space
            String columns[] = rwo.split("\t");
	    // Checking a condition if the string array length greater than 6 to eliminate the ArrayIndexOutOfBoundsException error. 
            if(columns.length>6){
		// setting the Rating value which is in 7th column
                rating.set(Float.parseFloat(columns[6]));
		// setting the Video Id value which is in 1st column
                videoId.set(columns[0]);
            }
	    // writing the key and value into the context
            context.write(videoId,rating_value);
        }
    }

    // extends the default Reducer class to take Text keyIn, FloatWritable ValueIn, Text keyOut and FloatWritable ValueOut.
    public static class Reduce extends Reducer<Text, FloatWritable,Text, FloatWritable> {
	// overriding the Reduce method that run for every key
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
	    // count to store the sum of all the values for each key
            float sum = 0;
		// varibale to increment with corresponding to values of particular key.
            int number = 0;
	    // Looping through the iterable values which are output of sort and shuffle phase
            for (FloatWritable val : values) {
		// Incrementing and calcluating sum for each key
                number += 1;
                sum += val.get();
            }
	    // Calcluating the average of the sum
            sum = sum / number;
	    // writing the key and value into the context
            context.write(key, new FloatWritable(sum));
        }
    }

	// Driver Program
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // Initializing the configuration
        Configuration conf1= new Configuration();
	// Initializing the Job 
        Job job = new Job(conf1,"You Tube Data analysis");
	// Setting the Jar class
        job.setJarByClass(YouTubeTopRated.class);
	// Setting the Mapper class
        job.setMapperClass(Map.class);
	// Setting the Reducer class
        job.setReducerClass(Reduce.class);
	// Setting the Output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
	// Setting the Input and Output Format classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
	 // set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
