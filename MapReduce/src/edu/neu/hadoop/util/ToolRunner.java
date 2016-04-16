package edu.neu.hadoop.util;


/**
 * A utility to help run {@link Tool}s.
 * 
 * <p>
 * <code>ToolRunner</code> can be used to run classes implementing
 * <code>Tool</code> interface. The application-specific options are passed
 * along without being modified.
 * </p>
 * 
 * @author Adib Alwani
 */
public class ToolRunner {

	/**
	 * Runs the <code>Tool</code> with its <code>Configuration</code>.
	 * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
	 * 
	 * @param tool <code>Tool</code> to run.
	 * @param args command-line arguments to the tool.
	 * @return exit code of the {@link Tool#run(String[])} method.
	 */
	public static int run(Tool tool, String[] args) throws Exception {
		return tool.run(args);
	}
}
