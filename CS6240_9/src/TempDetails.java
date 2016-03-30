
/**
 * Class representing Temperature details 
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class TempDetails implements Comparable<TempDetails> {

	private final int wBan;
	private final String date;
	private final int time;
	private final float temperature;
	
	public TempDetails(int wBan, String date, int time, float temperature) {
		this.wBan = wBan;
		this.date = date;
		this.time = time;
		this.temperature = temperature;
	}

	@Override
	public int compareTo(TempDetails tempDetails) {
		float temp = tempDetails.getTemperature();
		if (temperature > temp) {
			return -1;
		} else if (temperature < temp) {
			return 1;
		} else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(wBan);
		sb.append(',');
		sb.append(date);
		sb.append(',');
		sb.append(time);
		sb.append(',');
		sb.append(temperature);

		return sb.toString();
	}
	
	/**
	 * Get an instance of TempDetails from the given line
	 * 
	 * @param line Line information
	 * @return TempDetails instance
	 */
	public static TempDetails fromString(String line) {
		String[] data = line.split(",");
		int wBan = Integer.parseInt(data[0]);
		String date = data[1];
		int time = Integer.parseInt(data[2]);
		float temperature = Float.parseFloat(data[3]);
		return new TempDetails(wBan, date, time, temperature);
	}

	public int getwBan() {
		return wBan;
	}

	public String getDate() {
		return date;
	}

	public int getTime() {
		return time;
	}

	public float getTemperature() {
		return temperature;
	}
}
