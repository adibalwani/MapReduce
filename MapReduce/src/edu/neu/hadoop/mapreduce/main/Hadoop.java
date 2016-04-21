package edu.neu.hadoop.mapreduce.main;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.jar.JarFile;


/**
 * Main class to run the Map-Reduce framework in Pseudo mode
 * 
 * @author Rachit Puri, Adib Alwani
 */
public class Hadoop {

	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
		
		if (args.length < 3) {
			System.out.println("Run as: Hadoop [Program] [input path] [output path]");
			return;
		}
		
		String mainClassName = null;
		
		// Check whether JAR or Class file is provided
		if (args[0].contains(".jar")) {
			String jarFileName = args[0];

			try (
				JarFile jarFile = new JarFile(new File(jarFileName));
			) {
				if (jarFile.getManifest().getEntries().containsKey("Main-Class")) {
					mainClassName = jarFile.getManifest().getEntries()
							.get("Main-Class").getValue("Main-Class");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}	 
		} else {
			 mainClassName = args[0];
		}
		
		Class<?> cls = Class.forName(mainClassName);
		Method meth = cls.getMethod("main", String[].class);
		meth.invoke(null, (Object) Arrays.copyOfRange(args, 1, args.length));
		//Class.forName(mainClassName).newInstance();
		System.out.println("Starting Map Reduce Job");
	}
}
