package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//splits the key, value pair
		String[] K1 = value.toString().split("\t");
		
		//Splits the rank and out edges
		String[] temp = K1[1].split(";");
		for (String s : temp[0].split("/")) {
			String[] parts = s.split(",");
			if (parts[0].equals(K1[0])) {
				context.write(new Text(K1[0]), new Text(parts[1]));
			}
		}
	} 
}