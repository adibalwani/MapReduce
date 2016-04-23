import edu.neu.hadoop.conf.Configured;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.io.BytesWritable;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.mapreduce.Job;
import edu.neu.hadoop.mapreduce.lib.input.FileInputFormat;
import edu.neu.hadoop.mapreduce.lib.output.FileOutputFormat;
import edu.neu.hadoop.util.Tool;
import edu.neu.hadoop.util.ToolRunner;
import edu.neu.hadoop.mapreduce.Context;

/**
 * MapReduce program to predict flight delays
 * 
 * @author Adib Alwani
 */
public class Prediction extends Configured implements Tool {
	
	/**
	 * Run MapReduce job to build model
	 * 
	 * @param args Command line args
	 * @return Job report
	 * @throws Exception
	 */
	private boolean buildModel(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJar("job.jar");
		job.setMapperClass(ModelMapper.class);
		job.setReducerClass(ModelReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlightDetail.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true);
	}
	
	/**
	 * Run MapReduce job to test model
	 * 
	 * @param args Command line args
	 * @return Job report
	 * @throws Exception
	 */
	private boolean testModel(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJar("job.jar");
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlightDetail.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		return job.waitForCompletion(true);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		 return buildModel(args) && testModel(args) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new Prediction(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
