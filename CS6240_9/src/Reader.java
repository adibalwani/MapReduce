import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Class to read input folder
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class Reader {

	/**
	 * Return a list of temperature details from the given file or directory
	 * 
	 * @param fileName
	 *            The name of a text file or a folder
	 * @return List of temperature details
	 */
	public List<TempDetails> readFileOrDirectory(String fileName) {
		List<TempDetails> tempDetails = new ArrayList<TempDetails>(); 
		
		// gets the list of files in a folder (if user has submitted
		// the name of a folder) or gets a single file name (is user
		// has submitted only the file name)
		List<File> files = new ArrayList<File>();
		addFiles(new File(fileName), files);

		for (File file : files) {
			try (
				InputStream gzipStream = new GZIPInputStream(new FileInputStream(file));
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipStream));
			) {
				String currentLine;
				while ((currentLine = bufferedReader.readLine()) != null) {
					String[] row = parse(currentLine, 20);
					if (row != null && sanityTest(row)) {
						int wBan = (int) Float.parseFloat(row[0]);
						String date = row[1];
						int time = (int) Float.parseFloat(row[2]);
						float temperature = Float.parseFloat(row[8]);
						tempDetails.add(new TempDetails(wBan, date, time, temperature));
					}
				}
			} catch (Exception exception) {
				exception.printStackTrace();
			}
		}
		
		return tempDetails;
	}

	/**
	 * Add the file to the given List of files. 
	 * If the file is a directory search its files and sub-directories
	 * 
	 * @param file
	 *            The file or directory to add from
	 * @param files
	 *            The list of files to add to
	 */
	private void addFiles(File file, List<File> files) {
		// File or Directory doesn't exist
		if (!file.exists()) {
			System.out.println(file + " does not exist.");
			return;
		}

		// Provided a directory
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				addFiles(f, files);
			}
		} else {
			files.add(file);
		}
	}
	
	/**
	 * Parse a CSV record given as string
	 * - Remove any quotes from record
	 * - Splits on each new column
	 * 
	 * @param record The record to parse 
	 * @return Array containing those records, null if couldn't parse
	 */
	public String[] parse(String record, int column) {
		record = record.replaceAll("\"", "").replaceAll(",[\\s]", ";");
		return record.split(",");
	}
	
	/**
	 * Check whether the given temperature record passes the sanity test
	 * 
	 * @param row Record of temperature data
	 * @return true iff it passes sanity test. False, otherwise
	 */
	@SuppressWarnings("unused")
	public boolean sanityTest(String[] row) {
		if (row.length != 20) {
			return false;
		}
		
		try {
			int wBan = (int) Float.parseFloat(row[0]);
			String date = row[1];
			int time = (int) Float.parseFloat(row[2]);
			float temperature = Float.parseFloat(row[8]);
			
			// Check for non-empty condition
			if (date.isEmpty()) {
				return false;
			}
		} catch (NumberFormatException exception) {
			return false;
		}
		
		return true;
	}
}
