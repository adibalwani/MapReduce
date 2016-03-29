
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
	public String toString()
	{
		StringBuilder sb=new StringBuilder();
		sb.append(wBan)
		.append(",")
		.append(date)
		.append(",")
		.append(time)
		.append(",")
		.append(temperature);

		return sb.toString();
		
	}
}
