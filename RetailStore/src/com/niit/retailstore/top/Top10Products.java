package com.niit.retailstore.top;

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

/*
 * 
 */
public class Top10Products {

	
	/*
	 * Mapper code
	 */
	public static class TopMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			try {
			String[] part = value.toString().split(";");
			String productId = part[5];
			long amount = Integer.parseInt(part[8]);
			
			context.write(new Text(productId), new LongWritable(amount));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
		
	}
	
	/*
	 * Reducer code
	 */
	public static class TopReducer extends Reducer<Text, LongWritable, NullWritable, Text>
	{
		private TreeMap<LongWritable, Text> treeMap = new TreeMap<LongWritable, Text>();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException, InterruptedException 
		{
			long sum = 0;
			String myValue = "";
			String mySum = "";
			
			for(LongWritable val : values)
			{
				sum += val.get();
			}
			
			myValue = key.toString();
			mySum = String.format("%d", sum);
			
			// in myValue putting myValue and mySum 
			myValue = myValue + ", " +mySum;
			
			// myValue as value to TreeMap sum as key  to the TreeMap
			treeMap.put(new LongWritable(sum), new Text(myValue));
			
			// TreeMap Sort key by default in ascending order
			// when TreeMap size > 10 then remove first key
			if(treeMap.size() > 10)
			{
				treeMap.remove(treeMap.firstKey());
			}
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException 
		{
			// here we printing the TreeMap in descending order using descendingMap()
			for(Text top : treeMap.descendingMap().values())
			{
				// Writing only value in output of Reducer, key is NullWritable.get() it will not write anything in output 
				context.write(NullWritable.get(), top);
			}
		}
		
		
		
	}
	
	/*
	 * Driver code
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 10 Products");
		
		job.setJarByClass(Top10Products.class);
		job.setMapperClass(TopMapper.class);
		job.setReducerClass(TopReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
