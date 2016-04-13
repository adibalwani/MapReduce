package edu.neu.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.jar.JarFile;


/**
 * Main class to run the Map-Reduce framework
 * 
 * @author Rachit Puri, Adib Alwani
 */
public class Hadoop {

	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		
		if (args.length < 3) {
			System.out.println("Run as: hadoop [MainClass] [input path] [output path]");
			return;
		}
		
		String mainClassName = null;
		
		// Check whether JAR or Class file is provided
		if (args[1].contains(".jar")) {
			String jarFileName = args[1];

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
			 mainClassName = args[1];
		}
		
		Class.forName(mainClassName).newInstance();
	}
}
