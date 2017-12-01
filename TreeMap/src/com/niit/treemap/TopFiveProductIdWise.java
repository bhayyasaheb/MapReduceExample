package com.niit.treemap;

import java.io.IOException;
import java.util.TreeMap;

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

public class TopFiveProductIdWise {

	public static class Top5Mapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split(",");
			//int id = Integer.parseInt(token[0]);
			String id = token[0];
			
			context.write(new Text(id), new Text(value));
		}
		
	}
	
	public static class Top5Reducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> treeMap =  new TreeMap<Integer, Text>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int id = 0;
			String myValue = "";
			for(Text val : values)
			{
				String[] token = val.toString().split(",");
				 id = Integer.parseInt(token[0]);
				 //myValue = myValue + Integer.parseInt(token[1]);
				 myValue = token[1];
				
				 // in myValue putting id and value of that id and passing to the TreeMap
				 myValue = id + "," + myValue;
					
				 treeMap.put(id, new Text(myValue));
				
				 // TreeMap Sort key by default in ascending order
				 // when TreeMap size > 5 then remove first key
				 if(treeMap.size() > 5)
				 {
					treeMap.remove(treeMap.firstKey());
				 }	 
				 
			}
			
			
		}
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException 
		{
			try {
				// here we printing the TreeMap in descending order using descendingMap()
				for(Text top : treeMap.descendingMap().values())
				{	
					// Writing only value in output of Reducer, key is NullWritable.get() it will not write anything in output 
					context.write(NullWritable.get(), top);
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());			
				}
			
		}
		
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 5 Products using TreeMap key bases");
		
		job.setJarByClass(TopFiveProductIdWise.class);
		job.setMapperClass(Top5Mapper.class);
		job.setReducerClass(Top5Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
