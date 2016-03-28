import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Class used for randomly selecting samples from the input data
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class Sampling {
	
	private Random randomGenerator;
	
	public Sampling() {
		randomGenerator = new Random();
	}

	/**
	 * Return random samples from the given data 
	 * 
	 * @param tempDetails Temperature Details
	 * @return Samples of the data
	 */
	public List<TempDetails> sampleData(List<TempDetails> tempDetails) {
		List<TempDetails> samples = new ArrayList<TempDetails>(); 
		int len = tempDetails.size();
		int rootNumber = (int) Math.floor(Math.sqrt(len));
		
		for (int i = 0; i < rootNumber; i++) {
			int random = generateNumber(0, len);
			samples.add(tempDetails.get(random));
		}
		
		return samples;
	}
	
	/**
     * Returns a pseudo-random number between min and max.
     * Incudes Min, excludes Max
     *
     * @param min Minimum value
     * @param max Maximum value
     * @return number between min and max
     */
    private int generateNumber(int min, int max) {
        return randomGenerator.nextInt(max - min) + min;
    }
	
}
