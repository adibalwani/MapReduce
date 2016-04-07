package edu.neu.hadoop.util;

import Configurable;
import GenericOptionsParser;
import InterfaceAudience;
import InterfaceStability;
import ToolRunner;

/**
 * A tool interface that supports handling of generic command-line options.
 * 
 * <p><code>Tool</code>, is the standard for any Map-Reduce tool/application. 
 * The tool/application should delegate the handling of 
 * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options">
 * standard command-line options</a> to {@link ToolRunner#run(Tool, String[])} 
 * and only handle its custom arguments.</p>
 * 
 * <p>Here is how a typical <code>Tool</code> is implemented:</p>
 * <p><blockquote><pre>
 *     public class MyApp extends Configured implements Tool {
 *     
 *       public int run(String[] args) throws Exception {
 *         // <code>Configuration</code> processed by <code>ToolRunner</code>
 *         Configuration conf = getConf();
 *         
 *         // Create a JobConf using the processed <code>conf</code>
 *         JobConf job = new JobConf(conf, MyApp.class);
 *         
 *         // Process custom command-line options
 *         Path in = new Path(args[1]);
 *         Path out = new Path(args[2]);
 *         
 *         // Specify various job-specific parameters     
 *         job.setJobName("my-app");
 *         job.setInputPath(in);
 *         job.setOutputPath(out);
 *         job.setMapperClass(MyMapper.class);
 *         job.setReducerClass(MyReducer.class);
 *
 *         // Submit the job, then poll for progress until the job is complete
 *         RunningJob runningJob = JobClient.runJob(job);
 *         if (runningJob.isSuccessful()) {
 *           return 0;
 *         } else {
 *           return 1;
 *         }
 *       }
 *       
 *       public static void main(String[] args) throws Exception {
 *         // Let <code>ToolRunner</code> handle generic command-line options 
 *         int res = ToolRunner.run(new Configuration(), new MyApp(), args);
 *         
 *         System.exit(res);
 *       }
 *     }
 * </pre></blockquote></p>
 * 
 * @see GenericOptionsParser
 * @see ToolRunner
 */
public interface Tool extends Configurable {
  /**
   * Execute the command with the given arguments.
   * 
   * @param args command specific arguments.
   * @return exit code.
   * @throws Exception
   */
  int run(String [] args) throws Exception;
}

