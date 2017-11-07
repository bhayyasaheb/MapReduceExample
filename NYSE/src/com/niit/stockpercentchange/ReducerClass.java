package com.niit.stockpercentchange;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	DoubleWritable maxValue = new DoubleWritable();
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		double maxPercentValue =0;
		double temp_val = 0;
		
		for(DoubleWritable value :values)
		{
			temp_val = value.get();
			if(temp_val > maxPercentValue)
			{
				maxPercentValue = temp_val;
			}
		}
		maxValue.set(maxPercentValue);
		context.write(key, maxValue);
		//context.write(key, new DoubleWritable(maxPercentValue));
	}
	
}
