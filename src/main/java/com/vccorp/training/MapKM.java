package com.vccorp.training;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapKM extends Mapper<LongWritable, Text, Text, Text>{
	
	public static final Text a = new Text("A");
	public static final Text b = new Text("B");
	public static final Text c = new Text("C");
	private Text str = new Text();
	
	class Point{
		public double x;
		public double y;
		
		public Point(double d, double e){
			this.x = d;
			this.y = e;
		}
	}
	
	public static double Distance(Point A, Point B){
		double distan = Math.sqrt(Math.pow(A.x - B.x, 2) + Math.pow(A.y - B.y, 2));
		return distan;
	}
	@SuppressWarnings("unused")
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		String centorids = conf.get("centorids"); 
		String[] arrCentorids = centorids.split("\\s");
		
		Point centoridA = new Point(Double.parseDouble(arrCentorids[0]), Double.parseDouble(arrCentorids[1]));
		Point centoridB = new Point(Double.parseDouble(arrCentorids[2]), Double.parseDouble(arrCentorids[3]));
		Point centoridC = new Point(Double.parseDouble(arrCentorids[4]), Double.parseDouble(arrCentorids[5]));
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreElements()) {
			str.set(tokenizer.nextToken());
			String[] words = tokenizer.nextToken().split("\\s");
			Point inputPoint = new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1]));
			if(Distance(centoridA, inputPoint) < Distance(centoridB, inputPoint) && Distance(centoridA, inputPoint) < Distance(centoridC, inputPoint)){
					context.write(str, a);
			}
			if(Distance(centoridB, inputPoint) < Distance(centoridA, inputPoint) && Distance(centoridB, inputPoint) < Distance(centoridC, inputPoint)){
				context.write(str, b);
			}
			if(Distance(centoridC, inputPoint) < Distance(centoridB, inputPoint) && Distance(centoridC, inputPoint) < Distance(centoridA, inputPoint)){
				context.write(str, c);
			}
		}
	}
}