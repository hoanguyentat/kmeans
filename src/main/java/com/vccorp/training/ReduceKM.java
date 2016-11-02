package com.vccorp.training;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceKM extends Reducer{
	
	class Point{
		public double x;
		public double y;
		
		public Point(double d, double e){
			this.x = d;
			this.y = e;
		}
	}
	
	String pathOut = "/user/hoant/output/centoridOut.txt";
	
	@SuppressWarnings("unchecked")
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		Iterator<Text> ite = values.iterator();
		
		ArrayList<Point> arrPoint = new ArrayList<ReduceKM.Point>();
		
		while(ite.hasNext()){
			String[] words = ite.next().toString().split("\\s");
			arrPoint.add(new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1])));
		}
		double xco = 0, yco = 0;
		for(Point input: arrPoint){
			xco += input.x;
			yco += input.y;
		}
		xco = xco/(arrPoint.size());
		yco = yco/(arrPoint.size());
		
		FileOutputStream fos = new FileOutputStream(pathOut);
		PrintWriter pw = new PrintWriter(fos);
		pw.println(xco + " " + yco);
		try {
			context.write(key, new Text(xco + " " + yco));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
