package com.niit.retailstore.treemap;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 
 * 
 */
public class Top5LossCategoryAgeWise {

	
	/*
	 * Mapper runs on input retail data like D*
	 */
	public static class Top5ViableMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] part = value.toString().split(";");
			String category = part[4];
			
			// here key is category and value is whole value
			context.write(new Text(category), new Text(value));
		}
		
	}
	
	/*
	 * Partitioner for age wise partitioning data using Mapper output it will saves the output age wise
	 */
	public static class AgePartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			
			String[] str = value.toString().split(";");
			char age = str[2].charAt(0);
			
			if(age == 'A')
			{
				return 0 % numReduceTasks;
			}
			else if (age == 'B') {
				return 1 % numReduceTasks;
			}
			else if (age == 'C') {
				return 2 % numReduceTasks;
			}
			else if (age == 'D') {
				return 3 % numReduceTasks;
			}
			else if (age == 'E') {
				return 4 % numReduceTasks;
			}
			else if (age == 'F') {
				return 5 % numReduceTasks;
			}
			else if (age == 'G') {
				return 6 % numReduceTasks;
			}
			else if (age == 'H') {
				return 7 % numReduceTasks;
			}
			else if (age == 'I') {
				return 8 % numReduceTasks;
			}
			else if (age == 'J') {
				return 9 % numReduceTasks;
			}
			else {
				return 10 % numReduceTasks;
			}
			
		}
		
	}
	
	/*
	 * Reducer runs on Mapper output and partitioner output
	 */
	public static class Top5ViableReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		private TreeMap<Long,Text> repToRecordMap = new TreeMap<Long,Text>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			 long totalCost = 0;
			 long totalSales = 0;
			 String age = "";
			 for(Text val : values)
			 {
				 String[] str = val.toString().split(";");
				 long cost = Long.parseLong(str[7]);
				 long sales = Long.parseLong(str[8]);
				 age = str[2];
				 totalCost += cost;
				 totalSales += sales;
			 }
			 
			 long loss =totalCost - totalSales;
			 
			 // putting records in TreeMap if loss is +ve
			 if(loss > 0)
			 {
				// here key is  category which coming from mapper output key
				 String myValue = key.toString();
				 String myTotal = String.format("%d",loss);
				 
				// in myValue putting myValue=category, age, myTotal=loss which (difference between cost-sales)
				 myValue = myValue + ","+age+","+myTotal;
				 
				// in TreeMap key = loss and value = category,age,loss
				 repToRecordMap.put(new Long(loss), new Text(myValue));
				
				 if(repToRecordMap.size() > 5)
				 {
					// TreeMap by default sort key ascending order
					 // if TreeMap size > 5 then remove first key 
					 // that will remove lowest value of loss from TreeMap
					 repToRecordMap.remove(repToRecordMap.firstKey());
				 }
				// context.write(key, new LongWritable(loss));
			 }
			
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException 
		{
			// here we printing the TreeMap in descending order using descendingMap()
			for(Text top : repToRecordMap.descendingMap().values())
			{
				// Writing only value in output of Reducer, key is NullWritable.get() it will not write anything in output 
				context.write(NullWritable.get(), top);
			}
		}
		
	}
	
	
	/*
	 * Driver Code which contains configuration and job related all information
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Using TreeMap Top 5 Loss Making Product Age Wise Partitioner");
		
		job.setJarByClass(Top5LossCategoryAgeWise.class);
		job.setMapperClass(Top5ViableMapper.class);
		
		job.setPartitionerClass(AgePartitioner.class);
		job.setNumReduceTasks(11);
		
		job.setReducerClass(Top5ViableReducer.class);
	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}