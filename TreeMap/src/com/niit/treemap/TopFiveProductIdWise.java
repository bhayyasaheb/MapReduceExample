package com.niit.treemap;

import java.io.IOException;
import java.util.Map;
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

	public static class Top5Mapper extends Mapper<LongWritable, Text, NullWritable, Text>
	{
		TreeMap<Integer,Text> map1 = new TreeMap<Integer,Text>();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split(",");
			
			//id is productId
			int id = Integer.parseInt(token[0]);
			
			// myVal is productId and its value 
			String myVal = token[0]+","+token[1];
			
			// putting productId as key and value as myVal in TreeMap
			map1.put(id, new Text(myVal));
			
			// TreeMap Sort key by default in ascending order
			// when TreeMap size > 5 then remove first key
			if(map1.size()>5)
			{
				map1.remove(map1.firstKey());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			for(Map.Entry<Integer, Text> entry : map1.entrySet())
			{
				// putting whole TreeMap in value of cleanup method
				// value contains productId and its values
				context.write(NullWritable.get(), entry.getValue());
			}
		}
		
	}
	
	public static class Top5Reducer extends Reducer<NullWritable, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> map2 =  new TreeMap<Integer, Text>();
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int id = 0;
			
			for(Text val : values)
			{
				String[] token = val.toString().split(",");
				
				// id is productId
				id = Integer.parseInt(token[0]);
				 
				// putting productId as key and mapper output value as value in TreeMap
				map2.put(new Integer(id), new Text(val)); 
			}
			
			// here we printing the TreeMap values in descending order using descendingMap()
			for(Text t : map2.descendingMap().values())
			{
				// Writing only value in output of Reducer, key is NullWritable.get() it will not write anything in output
				context.write(NullWritable.get(), t);
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 5 Products using TreeMap key bases");
		
		job.setJarByClass(TopFiveProductIdWise.class);
		job.setMapperClass(Top5Mapper.class);
		job.setReducerClass(Top5Reducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
