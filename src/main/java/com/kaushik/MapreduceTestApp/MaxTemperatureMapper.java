package com.kaushik.MapreduceTestApp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	public static final int MISSING = 9999;

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String year = line.substring(0,4);
		int airTemperature;
		if(line.charAt(4) == '+') {
			airTemperature = Integer.parseInt(line.substring(5,9));
		}
		else {
			airTemperature = Integer.parseInt(line.substring(4,9));
		}
		String airQuality = line.substring(9,10);
		if (airTemperature != MISSING && airQuality.matches("1")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
	
	

}
