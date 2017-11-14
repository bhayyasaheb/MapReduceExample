package com.niit.mapjoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StateProductsSales {

	
	/*
	 * Map side
	 */
	public static class StateMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Map<String, String> map =  new HashMap<String, String>();

		/*
		 * Set up method for getting the store_master file from path variable using context.getConfiguration
		 * and FileSystem
		 */
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException 
		{
			// getcacheFiles return null
			URI[] files = context.getCacheFiles();
			
			Path p = new Path(files[0]);
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			//BufferedReader br = new BufferedReader(new FileReader(p.toString()));
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
			
			String line = br.readLine();
			
			while(line != null)
			{
				String[] token = line.split(",");
				String storeId = token[0];
				String state = token[2];
				 map.put(storeId, state);
				 
				 line = br.readLine();
			}
			br.close();
			
			if(map.isEmpty())
			{
				throw new IOException("MyError : Unable to load master_store");
			}
		}
		
		// Map method
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String row = value.toString();
			String[] token = row.split(",");
			String storeId = token[0];
			String state = map.get(storeId);
			String productId = token[1];
			
			int qty = Integer.parseInt(token[2]);
			String myKey = state + ","+productId;
			
			context.write(new Text(myKey), new IntWritable(qty));
		}

	}
	
	/*
	 * Partitioner for state wise product sales quantity
	 */
	public static class StatePartitioner extends Partitioner<Text, IntWritable>
	{

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String[] str = key.toString().split(",");
			String state = str[0];
			if(state.equals("MAH"))
			{
				return 0 % numReduceTasks;
			}
			else
			{
				return 1% numReduceTasks;
			}
		}
		
	}
	
	/*
	 * Reducer for getting the sum of product wise qty
	 */
	public static class StateReducer extends Reducer< Text, IntWritable, Text, IntWritable>
	{	
		IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for(IntWritable val :values)
			{
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat", ",");
		Job job = Job.getInstance(conf,"State wise Product sales qty");
		
		job.setJarByClass(StateProductsSales.class);
		job.setMapperClass(StateMapper.class);
		job.setPartitionerClass(StatePartitioner.class);
		job.setReducerClass(StateReducer.class);
		job.setNumReduceTasks(2);
		
		job.addCacheFile(new Path(args[1]).toUri());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
