package com.niit.chainmr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalTransPerProfession {

	/*
	 * 1. Customer Mapper for input args[0]
	 */
	public static class CustMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] part = value.toString().split(",");
			
			String customerId = part[0];
			
			String profession = part[4];
			
			String customerProfession = "c" + ","+ profession;
			
			// passing customerId as key and cutomerProfessin as value 
			// using String "c" as concatenation like customerProfession = c,pilot
			
			context.write(new Text(customerId), new Text(customerProfession));
		}
		
	}
	
	/*
	 * 2. Transaction Mapperfor input args[1]
	 */
	public static class TransMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split(",");
			
			String customerId = token[2];
			
			String price = token[3];
			
			String orderPrice = "t"+","+price;
			
			// passing customerId as key and cutomerProfessin as value 
			// using String "t" as concatenation like customerProfession = t,41222
			context.write(new Text(customerId), new Text(orderPrice));
		}
		
	}
	
	/*
	 * CustTransReducer  for Processing the CustMapper and TransMapper output
	 */
	public static class CustTransReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			String profession = "unknown";
			double totalPrice = 0.0;
			for(Text val : values)
			{
				String[] str = val.toString().split(",");
				String marker = str[0];
				
				if(marker.equals("c"))
				{
					profession = str[1];
				}
				else if(marker.equals("t"))
				{
					double orderPrice = Double.parseDouble(str[1]);
					totalPrice += orderPrice;
				}
			}
			
			// passing profession as key and totalPrice as value as Reducer output key and value
			context.write(new Text(profession), new DoubleWritable(totalPrice));
		}
		
	}
	
	/*
	 * Mapper 2 runs on output of CustTransReducer  here key is offset number and value is output from the CustsTransReducer which return using job1
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{

		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] part = value.toString().split("\t");
			
			String profession = part[0];
			double orderPrice = Double.parseDouble(part[1]);
			
			// sending key as profession and value as orderPrice to next Reducer input 
			context.write(new Text(profession), new DoubleWritable(orderPrice));
		}
		
	}
	
	/*
	 * Reducer 2 runs on output of mapper 2 (addition of values of price in this)
	 */
	public static class MyReducer extends Reducer<Text, DoubleWritable, DoubleWritable, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)throws IOException, InterruptedException 
		{
			double total = 0.0;
			for(DoubleWritable val : values)
			{
				total += val.get();
			}
			context.write(new DoubleWritable(total), key);
		}
		
	}
	
	/*
	 * Mapper 3 for sorting data it runs on MyReducer output key will be offset number and value is output from MyReducer which returns using job2 
	 */
	public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{

		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split("\t");
			
			double total = Double.parseDouble(token[0]);
			String profession = token[1];
			
			// sending key as total and value as profession to next Reducer input
			context.write(new DoubleWritable(total), new Text(profession));
		}
		
	}
	
	
	/*
	 * Reducer 3 runs on SortMapper output 
	 */
	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{

		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			for(Text val : values)
			{
				// Writing final output key as profession and value is total order price
				context.write(new Text(val), key);
			}
		}
		
	}
	
	
	/*
	 * Driver class code
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
	// First Job Runs on CustMapper, TransMapper and CustTransReducer
		Job job1 = Job.getInstance(conf,"Total Transaction per Profession");
		
		job1.setJarByClass(TotalTransPerProfession.class);
		job1.setReducerClass(CustTransReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TransMapper.class);
		
		//FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		Path outputPath1 = new Path("FirstMapper"); // This will be stored in hdfs like /user/hduser/FirstMapper
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1,true);
		
		job1.waitForCompletion(true);
		
		
	// Second Job Runs on MyMapper and MyReducer
		Job job2 = Job.getInstance(conf,"Total Transaction per Profession");
		
		job2.setJarByClass(TotalTransPerProfession.class);
		
		job2.setMapperClass(MyMapper.class);
		job2.setReducerClass(MyReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(Text.class);
		
		//FileInputFormat.addInputPath(job2, outputPath1);
		//FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		Path outputPath2 = new Path("SecondMapper"); // This will be stored in hdfs like /user/hduser/SecondMapper
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		
		FileSystem.get(conf).delete(outputPath2,true);
		
		job2.waitForCompletion(true);
		
		
	// Second Job Runs on SortMapper and SortReducer
		Job job3 = Job.getInstance(conf,"Total Transaction per Professin");
		
		job3.setJarByClass(TotalTransPerProfession.class);
		
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		
		job3.setSortComparatorClass(DecreasingComparator.class);
		
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job3, outputPath2);
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		
		FileSystem.get(conf).delete(new Path(args[2]), true);
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}

}