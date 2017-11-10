package com.niit.partitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyPartitioner {

	
	
	public static class PartitionerMappper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] str = value.toString().split(",");
			String gender = str[3];
			String name = str[1];
			String age = str[2];
			String salary = str[4];
			String myValue = name+","+age+","+salary;
			
			context.write(new Text(gender), new Text(myValue));
		}
		
	}
	
	public static class AgePartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) 
		{
			String[] str = value.toString().split(",");
			int age = Integer.parseInt(str[1]);
			
			if(age <= 20)
			{
				return 0 % numReduceTasks;
			}
			else if(age > 20 && age <= 30) 
			{
				return 1 % numReduceTasks; 
			}
			else
			{
				return 2 % numReduceTasks;
			}
		}
		
	}
	
	public static class PartitionerReducer extends Reducer<Text, Text, Text, IntWritable>
	{
		public int max;
		private Text outputKey = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			max = 0;
			for(Text val : values)
			{
				String[] str = val.toString().split(",");
				int salary = Integer.parseInt(str[2]);
				String name = str[0];
				String age = str[1];
				
				if(salary > max)
				{
					max = salary;
					String myKey = name+","+age;
					outputKey.set(myKey);
				}
			}
			context.write(outputKey, new IntWritable(max));
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Salaried Employees");
		
		job.setJarByClass(MyPartitioner.class);
		job.setMapperClass(PartitionerMappper.class);
		job.setPartitionerClass(AgePartitioner.class);
		job.setReducerClass(PartitionerReducer.class);
		job.setNumReduceTasks(3);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
		
	}

}
