package com.niit.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesPurchaseReduceJoin {

	// Mapper class for sales.txt
	public static class SalesMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split(",");
			String productId = token[0];
			String sales = token[1];
			
			context.write(new Text(productId), new Text("s."+sales));
		}		
	}
	
	// Mapper class for purchase.txt
	public static class PurchaseMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split(",");
			String productId = token[0];
			String purchase = token[1];
			
			context.write(new Text(productId), new Text("p."+purchase));
		}	
	}
	
	// Reducer class for both sales and purchase Mapper class output
	public static class SalesPurchaseReducer extends Reducer<Text, Text, Text, Text>
	{
		Text result = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			int s_qty = 0;
			int p_qty = 0; 
			for(Text val :values)
			{
				String[] str = val.toString().split("\\.");
				if(str[0].equals("s"))
				{
					s_qty = s_qty + Integer.parseInt(str[1]);
				}
				if(str[0].equals("p"))
				{
					p_qty = p_qty + Integer.parseInt(str[1]);
				}
			}
			result.set(p_qty+","+s_qty);
			context.write(key, result);
		}
		
	}
	
	// Main method
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sales Purchase of Product using Rducer join");
		
		job.setJarByClass(SalesPurchaseReduceJoin.class);
		job.setReducerClass(SalesPurchaseReducer.class);
		
		// adding multiple input from the terminal args[0] will be first file for input i.e. sales.txt
		// and args[1] will be second input file i.e purchase.txt
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PurchaseMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
