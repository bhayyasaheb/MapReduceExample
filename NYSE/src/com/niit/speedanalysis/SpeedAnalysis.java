package com.niit.speedanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpeedAnalysis {

	
	public static class SpeedMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] record = value.toString().split(",");
			String vehicleNo = record[0];
			double speed = Double.parseDouble(record[1]);
			
			context.write(new Text(vehicleNo), new DoubleWritable(speed));
		}
		
	}
	
	public static class SpeedReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
		{
			int count = 0;
			int offenceCount = 0;
			
			for(DoubleWritable value : values)
			{
				count++;
				if(value.get() > 65)
				{
					offenceCount++;
				}
			}
			double offence = (offenceCount * 100) / count;
			context.write(key, new DoubleWritable(offence));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Speed Analysis");
		job.setJarByClass(SpeedAnalysis.class);
		job.setMapperClass(SpeedMapper.class);
		job.setReducerClass(SpeedReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
