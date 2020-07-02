// Package definition
package com.bdp.youtube;

// importing all the necessary Java and Hadoop libraries

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
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

public class YouTubeAnalysis {
	// Extending the Mapper default class with keyIn as LongWritable and ValueIn as Text and KeyOut as Text and ValueOut as IntWritable.
    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
	// Declaring a Text type to store category of video
        private Text category = new Text();
	// Creating a static int variable which will be one always
        private final static IntWritable one = new IntWritable(1);
	// overriding map that runs for every line of input
        public void map(LongWritable key, Text value,
                        Context context) throws IOException,InterruptedException {
	    // Storing the each line and converting to string
            String row = value.toString();
	    // Splitting each record on tab space
            String colums[] = row.split("\t");
	    // Checking a condition if the string array length greater than 5 to eliminate the ArrayIndexOutOfBoundsException error. 
            if(colums.length>5){
		// setting the category value which is in 4th column
                category.set(colums[3]);
            }
	    // writing the key and value into the context
            context.write(category, one);
        }
    }

    // extends the default Reducer class to take Text keyIn, IntWritable ValueIn, Text keyOut and IntWritable ValueOut
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	// overriding the Reduce method that run for every key
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException,InterruptedException {
	    // count to store the sum of all the values for each key
            int count = 0;
	    // Looping through the iterable values which are output of sort and shuffle phase
            for(IntWritable value: values)
            {
		// Calculating the sum
                count+=value.get();
            }
	    // writing the key and value into the context
            context.write(key, new IntWritable(count));

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
        job.setJarByClass(YouTubeAnalysis.class);
	// Setting the Mapper class
        job.setMapperClass(Map.class);
	// Setting the Reducer class
        job.setReducerClass(Reduce.class);
	// Setting the Output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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