import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Routing extends Configured implements Tool {
	

	private boolean buildModel(String[] args) {
		Job job = Job.getInstance(getConf());
		job.setJar("job.jar");
		job.setMapperClass(RoutingMapper.class);
		job.setReducerClass(RoutingReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RoutingFlight.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	}
	
	// request file, input file
	private boolean PossibleConnections(String[] args) {
		Job job = Job.getInstance(getConf());
		job.setJar("job.jar");
		job.setMapperClass(ConnectionMapper.class);
		job.setReducerClass(ConnectionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RoutingFlight.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	}
	
	@Override
	public int run(String[] args) throws Exception {
		return buildModel(args) && PossibleConnections(args) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new Routing(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
