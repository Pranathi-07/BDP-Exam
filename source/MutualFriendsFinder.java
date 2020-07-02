// Package definition

package com.bdp.facebook;

// importing all the necessary Java and Hadoop libraries

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MutualFriendsFinder {
	// Extending the Mapper default class with keyIn as LongWritable , ValueIn as Text, KeyOut as Text and ValueOut as Text.
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	
        private Text pair = new Text(); // type of output key
        private Text friend_list = new Text(); // type of output value of mapper
	// overriding map that runs for every line of input
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    // Reading each line of input file and converting to string
            String[] line = value.toString().split(" -> ");
	    // Taking the first value i.e current friend
            String current_user = line[0];
            if (line.length ==2) {
		// Converting the firends array to an ArrayList
                ArrayList<String> FriendsList = new ArrayList<String>(Arrays.asList(line[1].split("\\,")));
                // Looping through the firendslist
                for(String Friend:FriendsList){
		    // comparing the friends and sorting the key so that the friends are in order
                    String FriendPair = (current_user.compareTo(Friend) < 0)? "(" + current_user + "," + Friend + ")" : "(" + Friend + "," + current_user + ")";

                    ArrayList<String> tmp = new ArrayList<String>(FriendsList);
                    tmp.remove(Friend);
                    String list = "";
		    // Appending the friend values to list
                    for (String t: tmp) {
                        list += t + ',';
                    }
		    // Removing the extra ',' in the end
                    list = list.substring(0, (listString.length() - 1));
		    // Setting the Pair as key and friends list as value
                    pair.set(FriendPair);
                    friend_list.set(list);
                    context.write(pair,friend_list);
                }
            }

        }
    }

    // extends the default Reducer class to take Text keyIn, Text ValueIn, Text keyOut and Text ValueOut

    public static class Reduce extends Reducer<Text, Text, Text, Text> { 
        private Text result = new Text(); // type of reducer output
	// overriding the Reduce method that run for every key
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Creating a hashmap instance to check the redundancy of key value
            HashMap<String, Integer> map = new HashMap<String, Integer>();
	    // To store the result
            StringBuilder sbuilder = new StringBuilder();
            sbuilder.append("(");
	    // Looping through the friends list of mapper output
            for (Text frnds : values) {
                List<String> temp_list = Arrays.asList(frnds.toString().split(","));
                for (String frnd : temp_list) {
                    if (map.containsKey(frnd))
                        sbuilder.append(frnd + ','); // append to string if friend already present
                    else
                        map.put(friend, 1);
                }
            }
	    // Deleting the last ','
            if (sbuilder.lastIndexOf(",") > -1) {
                sbuilder.deleteCharAt(sbuilder.lastIndexOf(","));
            }
            sbuilder.append(")");
	    // Setting the final key and result
            result.set(new Text(sbuilder.toString()));
            context.write(key, result);
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        // Initializing the configuration
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
	// Initializing the Job 
        Job job = new Job(conf, "Mutual Friend Finder");
	// Setting the Jar class
        job.setJarByClass(MutualFriendsFinder.class);
	// Setting the Mapper class
        job.setMapperClass(Map.class);
	// Setting the Reducer class
        job.setReducerClass(Reduce.class);
	// Setting the Output key class
        job.setOutputKeyClass(Text.class);
	// Setting the Output value class
        job.setOutputValueClass(Text.class);
        // set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Wait till job completion
        job.waitForCompletion(true);
    }
}
