package edu.neu.hadoop.mapreduce.main;

import java.lang.reflect.Method;
import java.util.Arrays;


/**
 * Main class to run the Map-Reduce framework in Pseudo mode
 * 
 * @author Rachit Puri, Adib Alwani
 */
public class Hadoop {

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Run as: Hadoop [Program] [input path] [output path]");
			return;
		}
		
		String mainClassName = null;
		
		// Check whether JAR or Class file is provided
		if (args[0].contains(".jar")) {
			mainClassName = args[0].split(".jar")[0];
		} else {
			 mainClassName = args[0];
		}
		
		Class<?> cls = Class.forName(mainClassName);
		Method meth = cls.getMethod("main", String[].class);
		meth.invoke(null, (Object) Arrays.copyOfRange(args, 1, args.length));
		System.out.println("Starting Map Reduce Job");
	}
}
