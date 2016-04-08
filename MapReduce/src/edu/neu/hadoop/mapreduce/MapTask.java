package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.lib.input.Reader;

public class MapTask {
	
	private Configuration conf;
	
	public MapTask(Configuration conf) {
		this.conf = conf;
	}
	
	/**
	 * Call {@link Mapper#map(Object, Object, edu.neu.hadoop.mapreduce.Mapper.Context)}
	 * on each input line, partition the emitted data (if any), sort and spill onto the
	 * disk
	 */
	public void run() {
		Path[] inputPaths = conf.getInputPath();
		Reader reader = new Reader();
		
		for (Path path : inputPaths) {
			reader.readFileOrDirectory(path.getPath(), new Reader.ReadListener() {
				@Override
				public void onReadLine(String line) {
					
				}
			});
		}
	}

}
