package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Text;

@SuppressWarnings("rawtypes")
public class MapperThread extends Thread {
	
	private Configuration conf;
	private Class<? extends Mapper> mapperClass;
	
	public MapperThread(Configuration conf) {
		this.conf = conf;
		mapperClass = conf.getMapperClass();
	}

	/**
	 * Call {@link Mapper#map(Object, Text, edu.neu.hadoop.mapreduce.Mapper.Context)}
	 * on each input line, partition the emitted data (if any), sort and spill onto the
	 * disk 
	 */
	@Override
	public void run() {
		try {
			Mapper mapper = (Mapper) Class.forName(mapperClass.getName()).newInstance();
			mapper.run(new MapContext(conf));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
