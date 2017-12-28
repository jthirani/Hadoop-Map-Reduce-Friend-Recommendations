package edu.upenn.nets212.hw3;

import java.io.IOException;
import java.security.KeyStore.Entry;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<String, Double> users = new TreeMap<String, Double>();
		String original = "";
		for (Text v : values) {
			if (!v.toString().contains("/")) {
				String[] parts = v.toString().split(",");
				if (users.containsKey(parts[0])) {
					double temp = users.get(parts[0]);
					users.replace(parts[0], temp + Double.parseDouble(parts[1]));
				} else {
					users.put(parts[0], Double.parseDouble(parts[1]));
				}
			} else {
				original = v.toString();
			}
		}
		String outs = "";
		for(Map.Entry<String, Double> e : users.entrySet()) {
			outs += e.getKey() + "," + e.getValue().toString() + "/";
		}
		outs += ";" + original;
		//emits the intermediate
		context.write(key, new Text(outs));
	}
  
}
