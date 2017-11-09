package com.niit.stdcallduration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class STDCall {

	
	
	// Mapper code
	
	public static class STDMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		Text phoneNumber = new Text();
		LongWritable durationInMin = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] str = value.toString().split(",");
			if(str[4].equals("1"))
			{
				String endTime = str[3];
				String startTime = str[2];
				
				// getting time in milliseconds
				long duration = toMillis(endTime) - toMillis(startTime);
				
				// converting time from milliseconds to seconds(by /1000) to minute(by /60)
				long totalDuration = (duration / (1000*60));
				
				phoneNumber.set(str[0]);
				durationInMin.set(totalDuration);
				
				context.write(phoneNumber, durationInMin);
			}
		}
		
		// For Converting the date and time in milliseconds from 1 Jan 1970
		private long toMillis(String date)
		{
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date dateFrm = null;
			
			try {
				dateFrm = format.parse(date);
				
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			return dateFrm.getTime();
		}
	}
	
	public static class STDReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		LongWritable result = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException 
		{
			long sum = 0;
			for(LongWritable val : values)
			{
				sum += val.get();
			}
			if(sum >= 60)
			{
				result.set(sum);
				context.write(key, result);
			}
		}
		
	}
	
	public static void main(String[] args) throws  Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf,"STDCalls Duration");
		
		job.setJarByClass(STDCall.class);
		job.setMapperClass(STDMapper.class);
		job.setReducerClass(STDReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}
