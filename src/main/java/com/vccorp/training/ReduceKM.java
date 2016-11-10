package com.vccorp.training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings("rawtypes")
public class ReduceKM extends Reducer<Text, Text, Text, Text>{
	
	Path pathOut = new Path("hdfs://localhost:9000/user/hduser/output/newCentorids/part-r-00000");
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Iterator<Text> ite = values.iterator();
		
		ArrayList<Point> arrPoint = new ArrayList<Point>();
		
		while(ite.hasNext()){
			String[] words = ite.next().toString().split("__");
			arrPoint.add(new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1])));
		}
		double xco = 0, yco = 0;
		for(Point input: arrPoint){
			xco += input.x;
			yco += input.y;
		}
		xco = xco/(arrPoint.size());
		yco = yco/(arrPoint.size());
		
		System.out.println(xco + " " + yco);
		
//		FileSystem fs = FileSystem.get(context.getConfiguration());
		try {
//			if(fs.exists(pathOut)){
//				fs.delete(pathOut, true);
//			}
//			FSDataOutputStream fin = fs.create(pathOut);
//			fin.writeUTF(xco + " " + yco + "\n");
//			fin.close();
			context.write(key, new Text(String.valueOf(xco) + " " + String.valueOf(yco)));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error in reducer");
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
	}
}
