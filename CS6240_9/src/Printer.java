import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;


/**
 * Helper class to print to file
 * 
 * @author Adib Alwani
 */
public class Printer {
	
	/**
	 * Print {@link TempDetails} to the given file
	 * 
	 * @param tempDetails The {@link TempDetails} to print
	 * @param fileName The name of the file to write to
	 */
	public static void printTempDetails(List<TempDetails> tempDetails, String fileName) {
		try (
			PrintWriter outputStream = new PrintWriter(new File(fileName));
		) {
			for (TempDetails tempDetail : tempDetails) {
				outputStream.println(tempDetail.toString());
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
