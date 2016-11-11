package com.vccorp.training;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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


public class Kmean{
	
	private static final int MAXITERATIONS = 4;
    private static final double THRESHOLD = 1;
    
//    private static Path newCentorids = new Path("hdfs://localhost:9000/user/hduser/output/newCentorids/part-r-00000");
//	private static Path oldCentorids = new Path("hdfs://localhost:9000/user/hduser/input/initCentorid/oldCentorids.txt");
	
	private static Path newCentorids = new Path("hdfs://10.3.24.154:9000/user/hoant/output/newCentorids/part-r-00000");
	private static Path oldCentorids = new Path("hdfs://10.3.24.154:9000/user/hoant/input/initCentorid/oldCentorids.txt");

	public static boolean stopIteration(Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		
		
		InputStreamReader isr1 = new InputStreamReader(fs.open(newCentorids));
		BufferedReader buf1 = new BufferedReader(isr1);
		
		InputStreamReader isr2 = new InputStreamReader(fs.open(oldCentorids));
		BufferedReader buf2 = new BufferedReader(isr2);
		
		Point oldCentorid, newCentorid;
		boolean stop = true;
		String line1, line2;
		while((line1 = buf1.readLine()) != null && (line2 = buf2.readLine()) != null){
			String []split1 = line1.split("\t");
			String []split2 = line2.split("\t");
			String []str1 = split1[1].split(" ");
			String []str2 = split2[1].split(" ");
			System.out.println(split1[1]);
			System.out.println(str2[1]);
			oldCentorid = new Point(Double.parseDouble(str1[0]), Double.parseDouble(str1[1]));
			newCentorid = new Point(Double.parseDouble(str2[0]), Double.parseDouble(str2[1]));
			if(Point.Distance(oldCentorid, newCentorid) > THRESHOLD){
				System.out.println("Chay trang cho nay lon hown threshold roi nhe");
				stop = false;
			}
			
			if(stop == false){
				fs.delete(oldCentorids, true);
				if(fs.rename(newCentorids, oldCentorids)==false)
		            {
		                System.exit(1);
		            }
				break;
			}
		}
		
		return stop;
	}
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		int interation = 1;
//		int success = 1;
		do{
				
			System.out.println("Job running good");
			
			
			
			FileSystem fs = FileSystem.get(conf);
			InputStreamReader isr = new InputStreamReader(fs.open(oldCentorids));
			BufferedReader br = new BufferedReader(isr);
			
			String buf = null;
			String stringCentorid = "";
			while((buf = br.readLine()) != null){
				String[] buf1 = buf.split("\t");
				stringCentorid = stringCentorid + buf1[1] + " ";
				System.out.println(stringCentorid);
			}
			conf.set("centorids", stringCentorid);
			
			Job job = new Job(conf, "kmean");
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapperClass(MapKM.class);
			job.setReducerClass(ReduceKM.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setNumReduceTasks(1);
			job.setJarByClass(Kmean.class);
			
			String input = "/user/hduser/input/kmeans";
			String output = "/user/hduser/output/newCentorids";
			if(fs.exists(new Path(output))){
				fs.delete(new Path(output), true);
			}
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			System.out.println("Chay roi nhe");
			interation++;
		} while(interation < MAXITERATIONS && (!stopIteration(conf)));
	}	
//	@SuppressWarnings({ "deprecation" })
//	public int run(String[] arg0) throws Exception {
//		System.out.println("Job running good");
//		
//		Path pathCentorids = new Path("hdfs://localhost:9000/user/hduser/input/initCentorid/oldCentorids.txt");
//		Configuration conf = new Configuration();
//		
//		FileSystem fs = FileSystem.get(conf);
//		InputStreamReader isr = new InputStreamReader(fs.open(pathCentorids));
//		BufferedReader br = new BufferedReader(isr);
//		
//		String buf = null;
//		String stringCentorid = "";
//		while((buf = br.readLine()) != null){
//			String[] buf1 = buf.split("\t");
//			stringCentorid = stringCentorid + buf1[1] + " ";
//			System.out.println(stringCentorid);
//		}
//		conf.set("centorids", stringCentorid);
//		
//		Job job = new Job(conf, "kmean");
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		
//		job.setMapperClass(MapKM.class);
//		job.setReducerClass(ReduceKM.class);
//		
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		
//		job.setNumReduceTasks(1);
//		job.setJarByClass(Kmean.class);
//		
//		String input = "/user/hduser/input/kmeans";
//		String output = "/user/hduser/output/newCentorids";
//		if(fs.exists(new Path(output))){
//			fs.delete(new Path(output), true);
//		}
//		
//		FileInputFormat.addInputPath(job, new Path(input));
//		FileOutputFormat.setOutputPath(job, new Path(output));
//		job.waitForCompletion(true);
//		
//		return job.waitForCompletion(true) ? 0 : 1;
//	}
}
