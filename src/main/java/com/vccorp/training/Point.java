package com.vccorp.training;

public class Point {
	public double x;
	public double y;
	
	public Point(double d, double e){
		this.x = d;
		this.y = e;
	}
	
	public static double Distance(Point A, Point B){
		double distan = Math.sqrt(Math.pow(A.x - B.x, 2) + Math.pow(A.y - B.y, 2));
		return distan;
	}
}
