package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String outs = key.toString() + ",1;";		
		for (Text v : values) {
			String[] parts = v.toString().split(",");
			outs += parts[0] + "," + parts[1] + "/";
		}
		
		
		//Emits (NodeID <tab> rank, out vertices separated by ';')
		context.write(key, new Text(outs));
	}
  
}
