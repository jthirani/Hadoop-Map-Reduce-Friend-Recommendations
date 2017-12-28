package edu.upenn.nets212.hw3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinishMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] kv = value.toString().split("\t");
		String[] pairs = kv[1].split(";");
		Map<String, Double> users = new TreeMap<String, Double>();
		String original = "";
		for (String v : pairs[0].split("/")) {
			String[] parts = v.split(",");
			users.put(parts[0], Double.parseDouble(parts[1]));
		}
		String outs = "For " + kv[0] + " : ";
		for(Map.Entry<String, Double> e : users.entrySet()) {
			outs += e.getKey() + " - " + e.getValue().toString() + ", ";
		}
		
		context.write(new Text("same"), new Text(outs));
	} 
}