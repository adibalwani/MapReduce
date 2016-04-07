package edu.neu.hadoop.util;


/**
 * A utility to help run {@link Tool}s.
 * 
 * <p>
 * <code>ToolRunner</code> can be used to run classes implementing
 * <code>Tool</code> interface. It works in conjunction with
 * {@link GenericOptionsParser} to parse the 
 * <a href="{@docRoot} to parse the <a href="{@docRoot}
 * /../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options">
 * generic hadoop command line arguments</a> and modifies the
 * <code>Configuration</code> of the <code>Tool</code>. The application-specific
 * options are passed along without being modified.
 * </p>
 * 
 * @see Tool
 * @see GenericOptionsParser
 */
public class ToolRunner {

	/**
	 * Runs the given <code>Tool</code> by {@link Tool#run(String[])}, after
	 * parsing with the given generic arguments. Uses the given
	 * <code>Configuration</code>, or builds one if null.
	 * 
	 * Sets the <code>Tool</code>'s configuration with the possibly modified
	 * version of the <code>conf</code>.
	 * 
	 * @param conf
	 *            <code>Configuration</code> for the <code>Tool</code>.
	 * @param tool
	 *            <code>Tool</code> to run.
	 * @param args
	 *            command-line arguments to the tool.
	 * @return exit code of the {@link Tool#run(String[])} method.
	 */
	public static int run(Configuration conf, Tool tool, String[] args)
			throws Exception {
		if (CallerContext.getCurrent() == null) {
			CallerContext ctx = new CallerContext.Builder("CLI").build();
			CallerContext.setCurrent(ctx);
		}

		if (conf == null) {
			conf = new Configuration();
		}
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		// set the configuration back, so that Tool can configure itself
		tool.setConf(conf);

		// get the args w/o generic hadoop args
		String[] toolArgs = parser.getRemainingArgs();
		return tool.run(toolArgs);
	}

	/**
	 * Runs the <code>Tool</code> with its <code>Configuration</code>.
	 * 
	 * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
	 * 
	 * @param tool
	 *            <code>Tool</code> to run.
	 * @param args
	 *            command-line arguments to the tool.
	 * @return exit code of the {@link Tool#run(String[])} method.
	 */
	public static int run(Tool tool, String[] args) throws Exception {
		return run(tool.getConf(), tool, args);
	}

}
