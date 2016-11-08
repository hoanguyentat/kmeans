package com.vccorp.training;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.vccorp.training.Point;


public class MapKM extends Mapper<LongWritable, Text, Text, Text>{
	
	public static final Text a = new Text("A");
	public static final Text b = new Text("B");
	public static final Text c = new Text("C");
	private Text str = new Text();
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Mapper running good");
		
		Configuration conf = context.getConfiguration();
		String centorids = conf.get("centorids");
		String hola = conf.get("hola");
		System.out.println(hola);
		String[] arrCentorids = centorids.split("\t");
		System.out.println("Mapper running good");
		System.out.println(arrCentorids);
		Point centoridA = new Point(Double.parseDouble(arrCentorids[0]), Double.parseDouble(arrCentorids[1]));
		Point centoridB = new Point(Double.parseDouble(arrCentorids[2]), Double.parseDouble(arrCentorids[3]));
		Point centoridC = new Point(Double.parseDouble(arrCentorids[4]), Double.parseDouble(arrCentorids[5]));
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreElements()) {
			str.set(tokenizer.nextToken());
			String[] words = tokenizer.nextToken().split("\t");
			Point inputPoint = new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1]));
			if(Point.Distance(centoridA, inputPoint) < Point.Distance(centoridB, inputPoint) && Point.Distance(centoridA, inputPoint) < Point.Distance(centoridC, inputPoint)){
					context.write(a, str);
			}
			if(Point.Distance(centoridB, inputPoint) < Point.Distance(centoridA, inputPoint) && Point.Distance(centoridB, inputPoint) < Point.Distance(centoridC, inputPoint)){
				context.write(b, str);
			}
			if(Point.Distance(centoridC, inputPoint) < Point.Distance(centoridB, inputPoint) && Point.Distance(centoridC, inputPoint) < Point.Distance(centoridA, inputPoint)){
				context.write(c, str);
			}
		}
	}
}