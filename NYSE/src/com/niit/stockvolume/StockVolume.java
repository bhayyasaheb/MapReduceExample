package com.niit.stockvolume;

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

public class StockVolume {

	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		
		private Text stock_id = new Text();
		private LongWritable volume = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) {
			
			try {
				String[] str = value.toString().split(",");
				String id = str[1];
				long vol = Long.parseLong(str[7]);
				
				stock_id.set(id);
				volume.set(vol);
				
				context.write(stock_id, volume);
				
			} catch (Exception e) {
				System.out.println("Exception Occurred : "+e.getMessage());
			}
		}
		
	}
	
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable result = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		
			long sum = 0;
			for(LongWritable value : values)
			{
				sum += value.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
		
		
	}
	
	
	public static void main(String[] args) throws Exception {
		

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Volume Count");
		job.setJarByClass(StockVolume.class);
		job.setMapperClass(MapClass.class);
		//job.setReducerClass(ReduceClass.class);
		
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
