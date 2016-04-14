package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Text;

/**
 * Thread class to spawn a new Mapper Implementation.
 * Call {@link Mapper#map(Object, Text, edu.neu.hadoop.mapreduce.Mapper.Context)} on
 * each input line, partition the emitted data (if any), sort and spill onto the
 * disk
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("rawtypes")
public class MapperThread extends Thread {
	
	private Configuration conf;
	private Class<? extends Mapper> mapperClass;
	
	public MapperThread(Configuration conf) {
		this.conf = conf;
		mapperClass = conf.getMapperClass();
	}

	@Override
	public void run() {
		System.out.println("Mapper Started");
		try {
			MapContext mapContext = new MapContext(conf);
			Mapper mapper = (Mapper) Class.forName(mapperClass.getName()).newInstance();
			mapper.run(mapContext);
			mapContext.sortPartitions();
			mapContext.spillToDisk();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Mapper Ended");
	}
}
