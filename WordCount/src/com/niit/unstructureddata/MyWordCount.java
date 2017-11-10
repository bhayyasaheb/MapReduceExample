package com.niit.unstructureddata;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class MyWordCount {

	public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static IntWritable one =  new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens())
			{
				String myWord = itr.nextToken().toLowerCase();
				
				word.set(myWord);
				context.write(word, one);
			}
		}
		
	}
	
	public static class CountReducer extends Reducer< Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf =  new Configuration();
		Job job = Job.getInstance(conf,"Processing the Unstructured data Word Count");
		
		job.setJarByClass(MyWordCount.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
