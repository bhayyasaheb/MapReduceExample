package com.niit.stockpercentchange;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable>
{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException	{
		try {
			String[] record = value.toString().split(",");
			String stockSymbol = record[1];
			double highVal = Double.valueOf(record[4]);
			double lowVal = Double.valueOf(record[5]);
			
			double percentChange = ((highVal-lowVal)*100)/ lowVal;
			
			context.write(new Text(stockSymbol), new DoubleWritable(percentChange));
			
		} catch (IndexOutOfBoundsException e) {
			System.out.println(e.getMessage());
		} catch (ArithmeticException e) {
			System.out.println(e.getMessage());
		}
	}
	
}
