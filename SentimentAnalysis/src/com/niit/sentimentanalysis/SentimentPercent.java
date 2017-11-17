package com.niit.sentimentanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentPercent {

	
	public static class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Map<String,String> map = new HashMap<String,String>();
		private final static IntWritable total_value = new IntWritable();
		private Text word = new Text();
		String myWord = "";
		int myValue= 0;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException 
		{
			URI[] files = context.getCacheFiles();
			Path p = new Path(files[0]);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			if(p.getName().equals("AFINN.txt"))
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
				String line = br.readLine();
				while(line != null)
				{
					String[] token = line.split("\t");
					// here putting word as key and value as its value of word in dictionary
					String diction_word = token[0];
					String diction_value = token[1];
					
					map.put(diction_word, diction_value);
					line = br.readLine();
				}
				br.close();
			}
			if(map.isEmpty())
			{
				throw new IOException("MyError : Unable to load data from Dictionary");
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			// using StringTokenizer we split records on space between them
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens())
			{
				// converting each token into lower case
				myWord = itr.nextToken().toLowerCase();
				// getting the value from dictionary using word which is another input file sentiment.txt
				if(map.get(myWord) != null)
				{
					myValue = Integer.parseInt(map.get(myWord));
					if(myValue > 0)
					{
						myWord = "positive";
					}
					else if(myValue < 0)
					{
						myWord = "negative";
						myValue = myValue * -1; 
					}
				}
				// if word is not in dictionary then setting key as positive and value as 0
				else
				{
					myValue = 0;
					myWord = "positive";
				}
				
				// setting positive negative as key and value as its value in dictionary
				word.set(myWord);
				total_value.set(myValue);
				context.write(word, total_value);
			}
		}
		
	}
	
	public static class SentimentReducer extends Reducer<Text, IntWritable, NullWritable, Text>
	{

		int pos_total = 0;
		int neg_total = 0;
		double sentPercent = 0.0;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for(IntWritable val : values)
			{
				sum +=val.get();
			}
			if(key.toString().equals("positive"))
			{
				pos_total = sum;
			}
			if(key.toString().equals("negative"))
			{
				neg_total = sum;
			}
		}

		// When we don't want to write output in reduce then we write output in cleanup method
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException 
		{
			// calculating sentiment percent using formula
			/*
			 * sentPercent = ((positive - negative) / (positive + negative)) *100 
			 */
			sentPercent = ( ((double) pos_total) - ((double) neg_total) ) / (((double) pos_total) + ((double) neg_total)) *100;
			String str = "Sentiment percent for the given text is " + String.format("%f", sentPercent);
			
			// writing output 
			context.write(NullWritable.get(), new Text(str));
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sentiment %");
		
		job.setJarByClass(SentimentPercent.class);
		job.setMapperClass(SentimentMapper.class);
		
		// adding index 1 files from terminal and send it to FileSystems in Mapper in Context method
		job.addCacheFile(new Path(args[1]).toUri());
		
		job.setReducerClass(SentimentReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
