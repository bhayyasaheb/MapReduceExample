package com.niit.retailstore.chainmr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top5LossProductAgeWise {

	/*
	 * First Mapper runs on input retail data like D*  
	 */
	public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] part = value.toString().split(";");
			String product = part[5]; 
			
			// passing product as key and value as value as Mapper output
			context.write(new Text(product), new Text(value));
		}
		
	}
	
	
	/*
	 * Partitioner1 for age wise partitioning data on FirstMapper output
	 */
	public static class FirstAgePartitioner extends Partitioner<Text, Text>
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
			else if (age == 'J'){
				return 9 % numReduceTasks;
			}
			else{
				return 10 % numReduceTasks; 
			}
			
		}
		
	}
	
	/*
	 * Reducer runs on FirstMapper output and FirstAgeParitioner output
	 */
	public static class FirstReducer extends Reducer<Text, Text, LongWritable, Text>
	{
		//private LongWritable myKey = new LongWritable();
		
		//private Text myValue = new Text();
		
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
			 
			 long loss = totalCost - totalSales;
			 
			// saving product and age to myKey here key is product with \t as delimiter
			 String myKey = key +"\t"+age;
			 
			 if(loss > 0)
			 {
				// writing reducer output when loss is +ve  
				 // key is difference between cost-sales and value is product and age as Reducer output
				 context.write(new LongWritable(loss), new Text(myKey));
			 }
			
		}
	}
	/*
	 * Mapper2 for sorting here input is FirstReducer input
	 */
	public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split("\t");
			
			// loss is token[0] as sortKey
			double sortKey =  Double.parseDouble(token[0]);
			
			// here token[1] is product & token[2] is age
			String sortValue = token[1]+"\t"+token[2];
			
			// Mapper output key is loss and output value is product and age
			context.write(new DoubleWritable(sortKey), new Text(sortValue));
		}
		
	}
	
	/*
	 * Partitioner 2 runs on SortMapper output
	 */
	public static class SortAgePartitioner extends Partitioner<DoubleWritable, Text>
	{

		@Override
		public int getPartition(DoubleWritable key, Text value, int numReduceTasks) {
			
			String[] str = value.toString().split("\t");
			char age = str[1].charAt(0);
			
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
			else if (age == 'J'){
				return 9 % numReduceTasks;
			}
			else{
				return 10 % numReduceTasks; 
			}
			
		}
		
	}
	
	/*
	 * REducer 2 for top 5 runs on SortMapper output and SortAgePartitioner
	 */
	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{
		int count = 0;
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException
		{
			if(count < 5)
			{
				for(Text val :values)
				{
					// Reducer output key is product & age and output value is loss
					// writing for only top5 loss
					context.write(new Text(val), key);
					count++;
				}
			}
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		// first job runs on retail data like D* using FirstMapper, FirstAgePartitioner & FirstReducer
		Job job1 = Job.getInstance(conf,"Using Chain MR Top 5 Loss  Making Product Age wise");
		
		job1.setJarByClass(Top5LossProductAgeWise.class);
		
		job1.setMapperClass(FirstMapper.class);
		job1.setPartitionerClass(FirstAgePartitioner.class);
		
		job1.setNumReduceTasks(11);
		job1.setReducerClass(FirstReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputPath1 = new Path("FirstMapper3"); // This will be stored in hdfs like /user/hduser/FirstMapper3
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1, true);
		
		job1.waitForCompletion(true);

		
		// second job runs using SortMapper, SortAgePartitioner and SortReducer
		Job job2 = Job.getInstance(conf,"Using Chain MR Top 5 Loss Making Product Age wise");
		
		job2.setJarByClass(Top5LossProductAgeWise.class);
		
		job2.setMapperClass(SortMapper.class);
		job2.setPartitionerClass(SortAgePartitioner.class);
		
		job2.setNumReduceTasks(11);
		job2.setReducerClass(SortReducer.class);
		
		job2.setSortComparatorClass(DecreasingComparator.class);
		
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		

		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		FileSystem.get(conf).delete(new Path(args[1]), true);
		
		System.exit(job2.waitForCompletion(true) ? 0 :1);		
	}


}