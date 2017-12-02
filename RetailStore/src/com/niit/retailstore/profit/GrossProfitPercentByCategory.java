package com.niit.retailstore.profit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * GrossProfitPercentByCategory
 */

public class GrossProfitPercentByCategory {

	public static class GrossMapperCategory  extends Mapper< LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] part = value.toString().split(";");
			String category = part[4];
			String cost = part[7];
			String sales = part[8];
			
			String myValue = cost+","+sales;
			context.write(new Text(category), new Text(myValue));			
			
		}
		
	}
	
	public static class GrossReducerCategory extends Reducer<Text, Text, Text, Text>
	{
		private Text result =  new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			
			long sumCost = 0;
			long sumSales = 0;
			long percentProfit = 0;
			
			try
			{			
				for(Text val : values)
				{
					String[] token = val.toString().split(",");
					long cost = Long.parseLong(token[0]);
					long sales = Long.parseLong(token[1]); 
					
					sumCost += cost;
					sumSales += sales;
					
					long profit = sumSales - sumCost;
					
					long profit100 = profit *100;
					
					percentProfit = profit100 / sumCost;
				}
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
			
			String grossPercentProfit = percentProfit + "%";
			
			result.set(grossPercentProfit);
			
			context.write(key,result);
			
			
		}
		
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Gross Profit by category id");
		
		job.setJarByClass(GrossProfitPercentByCategory.class);
		job.setMapperClass(GrossMapperCategory.class);
		job.setReducerClass(GrossReducerCategory.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}