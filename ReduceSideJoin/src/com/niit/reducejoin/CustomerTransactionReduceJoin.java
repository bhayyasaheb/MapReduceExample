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

public class CustomerTransactionReduceJoin {

	public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split(",");
			String customerId = token[0];
			String firstName = token[1];
			
			context.write(new Text(customerId), new Text("cust\t"+firstName));
		}	
	}
	
	public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token =  value.toString().split(",");
			String customerId = token[2];
			String amount = token[3];
			
			context.write(new Text(customerId), new Text("trans\t"+amount));
		}
	}
	
	public static class CustTransReducer extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			String name = "";
			int count = 0;
			double total = 0.0;
			for(Text val : values)
			{
				String[] str = val.toString().split("\t");
				if(str[0].equals("cust"))
				{
					name = str[1];
				}
				else if(str[0].equals("trans"))
				{
					count++;
					total = total + Double.parseDouble(str[1]);
				}
			}
			
			String result = count + ","+ total;
			
			context.write(new Text(name),new Text(result));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Customer Transaction Using Reduce side join");
		
		job.setJarByClass(CustomerTransactionReduceJoin.class);
		job.setReducerClass(CustTransReducer.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
		
	}

}
