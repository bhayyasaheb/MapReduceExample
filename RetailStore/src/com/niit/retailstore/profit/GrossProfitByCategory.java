package com.niit.retailstore.profit;

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

public class GrossProfitByCategory {

	
	public static class ProfitMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] part = value.toString().split(";");
			
			String categoryId = part[4];
			
			int totalCost = Integer.parseInt(part[7]);
			int totalSales = Integer.parseInt(part[8]);
			
			int profit = totalSales - totalCost;
			
			context.write(new Text(categoryId), new IntWritable(profit));
		}
		
	}
	
	
	public static class ProfitReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum =0;
			
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			
			context.write(new Text(key), new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Gross Profit By Product Id");
		
		job.setJarByClass(GrossProfitByCategory.class);
		job.setMapperClass(ProfitMapper.class);
		job.setReducerClass(ProfitReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
