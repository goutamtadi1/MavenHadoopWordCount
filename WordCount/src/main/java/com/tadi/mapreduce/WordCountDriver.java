package com.tadi.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountDriver extends Configured implements Tool {
	
	public static void main(String args[]) throws Exception {

		System.exit( ToolRunner.run(new WordCountDriver(), args));
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();// mandatory
		conf.set("","");
		Job job = new Job(conf);// mandatory
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(arg0[1]), true);		
		
		job.setJarByClass(WordCountDriver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(WordCountMapper.class); // mandatory
		//job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);// mandatory
		job.setOutputFormatClass(TextOutputFormat.class);// mandatory
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));// mandatory
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));// mandatory
		
		System.out.println(conf.get("mapred.reduce.tasks"));
		return job.waitForCompletion(true)?0:1;// mandatory
	}


}
