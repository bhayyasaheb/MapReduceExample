package com.niit.image;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ImageDuplicatesReducer extends Reducer<Text, Text, Text, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		/* key here is the md5 hash while the values are all the image files that
		 * are associated with it. for each md5 value we need to take only 
		 * one file  (the first)
		 */
		
		Text imageFilePath = null;
		for(Text filePath : values)
		{
			imageFilePath = filePath;
			// only the first one 
			// for one key we find only one value then break it.
			break;
		}
		
		// in the result file key will be the again the image file path.
		context.write(new Text(imageFilePath), NullWritable.get());
	}

	
}