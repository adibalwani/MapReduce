package edu.neu.hadoop.util;

import edu.neu.hadoop.conf.Configurable;

/**
 * A tool interface that supports handling of generic command-line options.
 * 
 * <p><code>Tool</code>, is the standard for any Map-Reduce tool/application. 
 * The tool/application should delegate the handling of 
 * standard command-line options</a> to {@link ToolRunner#run(Tool, String[])} 
 * and only handle its custom arguments.</p>
 * 
 * @author Adib Alwani
 */
public interface Tool extends Configurable {

	/**
	 * Execute the command with the given arguments.
	 * 
	 * @param args command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 */
	int run(String[] args) throws Exception;
}

