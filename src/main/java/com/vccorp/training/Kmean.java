package com.vccorp.training;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vccorp.training.Kmean.Point;

public class Kmean extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new Kmean(), args);
		System.exit(1);
	}
	
	class Point{
		public double x;
		public double y;
		
		public Point(double d, double e){
			this.x = d;
			this.y = e;
		}
	}
	
	@SuppressWarnings("resource")
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		
		
		String path = "/user/hoant/output/centorid.txt";
		
		FileInputStream fis = new FileInputStream(path);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
//		ArrayList<Kmean.Point> centorids = new ArrayList<Point>();
		
		String buf = null;
		String stringCentorid = "";
		while((buf = br.readLine()) != null){
			stringCentorid = stringCentorid + buf + " ";
//			String[] words = buf.split("\\s");
//			Float xco = Float.parseFloat(words[0]);
//			Float yco = Float.parseFloat(words[1]);
//			Point centorid = new Point(xco, yco);
//			centorids.add(centorid);
		}
		conf.set("centorids", stringCentorid);
		
		Job job = new Job(conf, "kmean");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapKM.class);
		job.setReducerClass(ReduceKM.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(9);
		job.setJarByClass(Kmean.class);
		
		String input = "/user/hoant/input";
		String output = "/user/hoant/output";
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
