package com.niit.retailstore.sales;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestTransaction {

	
	public static class HighestSaleMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] part = value.toString().split(";");
			
			String myKey = "all";
			
			String date = part[0];
			String customerId = part[1];
			String sales = part[8];
			
			String myValue = date+","+customerId+","+sales;
			
			context.write(new Text(myKey), new Text(myValue));
		}
		
	}
	
	public static class HighestSaleReducer extends Reducer<Text, Text, NullWritable, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			long maxValue = 0;
			String customerId = "";
			String date = "";
			
			for(Text val : values)
			{
				String[] str = val.toString().split(",");
				
				long highTrans = Long.parseLong(str[2]);
				String date1 = str[0];
				String customerID = str[1];
				
				if(highTrans > maxValue)
				{
					maxValue = highTrans;
					customerId = customerID;
					date = date1;
				}
			}
			String myValue = date+","+customerId+","+maxValue;
			
			context.write(NullWritable.get(), new Text(myValue));
			
		}
		
	}
	 
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Highest Transcation");
		
		job.setJarByClass(HighestTransaction.class);
		job.setMapperClass(HighestSaleMapper.class);
		job.setReducerClass(HighestSaleReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
