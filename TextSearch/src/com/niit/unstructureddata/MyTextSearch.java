package com.niit.unstructureddata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTextSearch {

	public static class SearchMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text sentence = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String mySearchText = context.getConfiguration().get("myText");
			String newText = mySearchText.toLowerCase();
			
			String line = value.toString();
			String newLine = line.toLowerCase();
			
			if(mySearchText != null)
			{
				if(newLine.contains(newText))
				{
					sentence.set(newLine);
					context.write(sentence, one);
				}
			}
		}
		
	}
	
	public static class SearchReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum =0;
			
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		
		if(args.length > 2)
		{
			conf.set("myText", args[2]);
		}
		else
		{
			System.out.println("Number of arguments should be 3");
			System.exit(0);
		}
		
		Job job = Job.getInstance(conf,"Text Search in a file");
		
		job.setJarByClass(MyTextSearch.class);
		job.setMapperClass(SearchMapper.class);
		job.setReducerClass(SearchReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
