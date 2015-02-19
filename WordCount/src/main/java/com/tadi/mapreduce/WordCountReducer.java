package com.tadi.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		IntWritable total= new IntWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			while (values.iterator().hasNext()) {

				sum += values.iterator().next().get();
							
			}
			total.set(sum);
			context.write(key, total);
		}
	}