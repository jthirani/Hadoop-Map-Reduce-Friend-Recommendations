package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	/*
	 * My intermediate - NodeID <tab> rank, all the out edge vertices separated by ';'
	 * 
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Splits the key, value pair
		String[] kv = value.toString().split("\t");
		String[] pairs = kv[1].split(";");
		context.write(new Text(kv[0]), new Text(pairs[1]));
		int numEdges = 0;
		for (String s : pairs[1].split("/")) {
			String[] t = s.split(",");
			numEdges += Integer.parseInt(t[1]);
		}
		
		for (String s : pairs[1].split("/")) {
			String[] p1 = s.split(",");
			double num = Double.parseDouble(p1[1]);
			for (String s1 : pairs[0].split("/")) {
				if (!s1.equals("")) {
					String[] p = s1.split(",");
					double temp = Double.parseDouble(p[1]);
					double result = num / numEdges * temp; 
					context.write(new Text(p1[0]), new Text(p[0] + "," + result));
				}
			}
		}
	}
}