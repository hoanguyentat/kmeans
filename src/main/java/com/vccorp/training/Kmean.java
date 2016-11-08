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


public class Kmean extends Configured implements Tool{
	
	private static final int MAXITERATIONS = 6;
    private static final double THRESHOLD = 1;
//    private static boolean StopSignalFromReducer=false;
//    private static int NoChangeCount=0;

	public static boolean stopIteration(Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		Path newCentorids = new Path("hdfs://localhost:9000/user/hduser/output/newCentorids/part-r-00000");
		Path oldCentorids = new Path("hdfs://localhost:9000/user/hduser/input/initCentorid/oldCentorids.txt");
		
		InputStreamReader isr1 = new InputStreamReader(fs.open(newCentorids));
		BufferedReader buf1 = new BufferedReader(isr1);
		
		InputStreamReader isr2 = new InputStreamReader(fs.open(oldCentorids));
		BufferedReader buf2 = new BufferedReader(isr2);
		
		Point oldCentorid, newCentorid;
		boolean stop = true;
		String line1, line2;
		while((line1 = buf1.readLine()) != null && (line2 = buf2.readLine()) != null){
			String []str1 = line1.split(" ");
			String []str2 = line2.split(" ");
			oldCentorid = new Point(Double.parseDouble(str1[0]), Double.parseDouble(str1[1]));
			newCentorid = new Point(Double.parseDouble(str2[0]), Double.parseDouble(str2[1]));
			if(Point.Distance(oldCentorid, newCentorid) > THRESHOLD){
				stop = false;
				break;
			}
			
			if(stop == false){
				fs.delete(oldCentorids, true);
				fs.rename(newCentorids, oldCentorids);
			}
		}
		
		return stop;
	}
	
	
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		
		
		int interation = 1;
		int success = 1;
		do{
			success ^= ToolRunner.run(conf, new Kmean(), args);
			System.exit(1);
		} while(success == 1 && interation < MAXITERATIONS && (!stopIteration(conf)));
		
		Job job = new Job(conf, "kmean");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapKM.class);
		job.setReducerClass(ReduceKM.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);
		job.setJarByClass(Kmean.class);
		
		String input = "/user/hduser/input/kmeans";
		Path output = new Path("/user/hduser/final");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, output);
	}
	
	@SuppressWarnings({ "deprecation" })
	public int run(String[] arg0) throws Exception {
		System.out.println("Job running good");
		
		Path pathCentorids = new Path("hdfs://localhost:9000/user/hduser/input/initCentorid/oldCentorids.txt");
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		InputStreamReader isr = new InputStreamReader(fs.open(pathCentorids));
		BufferedReader br = new BufferedReader(isr);
		
		String buf = null;
		String stringCentorid = "";
		while((buf = br.readLine()) != null){
			stringCentorid = stringCentorid + buf + "\t";
			System.out.println(stringCentorid);
		}
		
		conf.set("hola", "Nguyen Tat Hoa");
		conf.set("centorids", stringCentorid);
		
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		
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
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
