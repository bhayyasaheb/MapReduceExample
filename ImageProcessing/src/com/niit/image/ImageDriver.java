package com.niit.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ImageDriver extends Configured implements Tool{


	public int run(String[] args) throws Exception {
		
		// getting the configuration object and setting the job name
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Unique Images");
		
		// setting the class names
		job.setJarByClass(ImageDriver.class);
		job.setMapperClass(ImageDuplicatesMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(ImageDuplicatesReducer.class);
		//job.setNumReduceTasks(0);
		
		//setting output data type and classes 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// to accept the hdfs input and output dir at run time
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new ImageDriver(), args);
		System.exit(res);
	}

}
