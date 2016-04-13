package edu.neu.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.jar.JarFile;

public class Hadoop {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Run as: hadoop [MainClass] [input path] [output path]");
			return;
		}
		String mainClassName = args[1];
		// Use in case MainClass is given in MENIFEST.MF
		/*JarFile jf;
		try {
			jf = new JarFile(new File("job.jar"));
			if(jf.getManifest().getEntries().containsKey("Main-Class")) {
			    String mainClassName = jf.getManifest().getEntries().get("Main-Class").getValue("Main-Class");
			    System.out.println(mainClassName);
			};
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		Class.forName(mainClassName).main();
	}
}
