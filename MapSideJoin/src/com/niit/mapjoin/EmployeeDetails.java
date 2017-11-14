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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmployeeDetails {

	
	public static class EmpoyeeMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		private Map<String, String> map1 = new HashMap<String, String>();
		private Map<String, String> map2 = new HashMap<String, String>();
		
		private Text outputkey =  new Text();
		private Text outputvalue = new Text();
		
		
		/*
		 * 
		 * SetUp method for Map side Join
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			// getCacheFiles return null
			URI[] files = context.getCacheFiles();
			
			Path p1 = new Path(files[0]);
			Path p2 = new Path(files[1]); 
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			// reading data from salary.txt and storing it on HashMap 
			if(p1.getName().equals("salary.txt"))
			{
				//BufferedReader br = new BufferedReader(new FileReader(p1.toString()));
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p1)));
				
				String line = br.readLine();
				while(line != null)
				{
					String[] token = line.split(",");
					String empId = token[0];
					String empSalary = token[1];
					map1.put(empId, empSalary);
					
					line = br.readLine();
				}
				br.close();
			}
			
			// reading data from desig.txt and storing it on HashMap 
			if(p2.getName().equals("desig.txt"))
			{
				//BufferedReader br = new BufferedReader(new FileReader(p2.toString()));
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p2)));
				
				String line = br.readLine();
				
				while(line != null)
				{
					String[] token = line.split(",");
					String empId = token[0];
					String empDesig = token[1];
					map2.put(empId, empDesig);
					
					line = br.readLine();
				}
				br.close();
			}
			
			// if HashMap is empty can not load file then shows exception
			if(map1.isEmpty())
			{
				throw new IOException("MyError: Unable to load the salary.txt");
			}
			
			// if HashMap is empty can not load file then shows exception
			if(map2.isEmpty())
			{
				throw new IOException("MyError: Unable to load the desig.txt");
			}
			
		}

		/*
		 * Map Method
		 * 
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			// reading data from Employee.txt
			String row = value.toString();
			String[] token = row.split(",");
			String empId = token[0];
			String empSalary = map1.get(empId);
			String empDesig = map2.get(empId);
			
			String salaryDesig = empSalary +","+ empDesig;
			
			outputkey.set(row);
			outputvalue.set(salaryDesig);
			context.write(outputkey, outputvalue);
		}
		
	}
	
	
	/*
	 *  main method
	 */
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf,"Map side Join for Employee Details");
		
		job.setJarByClass(EmployeeDetails.class);
		job.setMapperClass(EmpoyeeMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.addCacheFile(new Path(args[1]).toUri());
		job.addCacheFile(new Path(args[2]).toUri());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
