import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * @author Adib Alwani
 */
public class LinearRegression extends Configured implements Tool {
	
	static class M extends Mapper<Object, Text, Text, Text> {
		
		/**
		 * Parse a CSV record given as string
		 * - Remove any quotes from record
		 * - Splits on each new column
		 * 
		 * @param record The record to parse 
		 * @return Array containing those records, null if couldn't parse
		 */
		private String[] parse(String record, int column) {
			
			String[] ans = new String[column];
			int col = 0;
			int len = record.length();
			StringBuilder builder = new StringBuilder();
			char[] arr = record.toCharArray();
			
			for (int i = 0; i < len; i++) {
				char ch = arr[i];
				
				// Remove quotes 
				if (ch == '\"') {
					continue;
				}
				
				if (ch != ',') {
					// No Split condition
					builder.append(ch);
					
					if (i == len - 1) {
						ans[col] = builder.toString();
					}
				} else if (i + 1 < len && arr[i + 1] == ' ') {
					// Ignore condition
					builder.append(ch);
				} else {
					// Split condition
					if (i == len - 1) {
						col = 0;
						break;
					}
					
					ans[col] = builder.toString();
					builder.setLength(0);
					col++;
				}
			}
			
			if (col < column - 1) {
				return null;
			}
			
			return ans;
		}
		
		/**
		 * Checks if the flight is within or on that year
		 * 
		 * @param row The record for a flight
		 * @param years Range of year or a particular year
		 * @return true if the flight is on given date, false otherwise
		 */
		private boolean checkYear(String row[], int... years) {
			int year = (int) Float.parseFloat(row[0]);
			
			if (years.length == 1 && year == years[0]) {
				return true;
			}
			
			if (years.length == 2 && year >= years[0] && year <= years[1]) {
				return true;
			}
			
			return false;
		}
		
		/**
		 * Convert time in hhmm format to minutes since day started
		 * 
		 * @param time The time in hhmm format
		 * @return Minutes minutes since day started
		 * @throws NumberFormatException
		 */
		private int timeToMinute(String time) throws NumberFormatException {
			if (time == null || time.length() == 0) {
				throw new NumberFormatException();
			}
			
			int hhmm = (int) Float.parseFloat(time);
			int hour = 0;
			int minute = 0;
			
			if (hhmm < 100) {
				minute = hhmm;
			} else {
				hour = hhmm / 100;
				minute = hhmm % 100; 
			}
			
			return hour * 60 + minute;
		}
		
		/**
		 * Check whether the given record passes the sanitary test
		 * 
		 * @param row Record of flight OTP data
		 * @return true iff it passes sanity test. False, otherwise
		 */
		private boolean sanityTest(String[] row) {
			try {
				
				// hh:mm format
				int CRSArrTime = timeToMinute(row[40]);
				int CRSDepTime = timeToMinute(row[29]);
				
				// Check for zero value
				if (CRSArrTime == 0 || CRSDepTime == 0) {
					return false;
				}
				
				// minutes format
				int CRSElapsedTime = (int) Float.parseFloat(row[50]);
				int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
				
				// Check for modulo zero
				if (timeZone % 60 != 0) {
					return false;
				}
				
				int originAirportId = (int) Float.parseFloat(row[11]);
				int destAirportId = (int) Float.parseFloat(row[20]);
				int originAirportSeqId = (int) Float.parseFloat(row[12]);
				int destAirportSeqId = (int) Float.parseFloat(row[21]);
				int originCityMarketId = (int) Float.parseFloat(row[13]);
				int destCityMarketId = (int) Float.parseFloat(row[22]);
				int originStateFips = (int) Float.parseFloat(row[17]);
				int destStateFips = (int) Float.parseFloat(row[26]);
				int originWac = (int) Float.parseFloat(row[19]);
				int destWac = (int) Float.parseFloat(row[28]);
				
				// Check for Ids greater than zero
				if (originAirportId <= 0 || destAirportId <= 0 || originAirportSeqId <= 0 || 
						destAirportSeqId <= 0 || originCityMarketId <= 0 || destCityMarketId <= 0 || 
						originStateFips <= 0 || destStateFips <= 0 || originWac <= 0 || destWac <= 0) {
					return false;
				}
				
				// Check for non-empty condition
				if (row[14].isEmpty() || row[23].isEmpty() || row[15].isEmpty() || row[24].isEmpty() ||
						row[16].isEmpty() || row[25].isEmpty() || row[18].isEmpty() || row[27].isEmpty()) {
					return false;
				}
				
				// For flights that are not cancelled
				int cancelled = (int) Float.parseFloat(row[47]);
				if (cancelled != 1) {
					int arrTime = timeToMinute(row[41]);
					int depTime = timeToMinute(row[30]);
					int actualElapsedTime = (int) Float.parseFloat(row[51]);
					
					// Check for zero value
					int time = arrTime - depTime - actualElapsedTime - timeZone;
					if (time != 0 && time % 1440 != 0) {
						return false;
					}
					
					int arrDelay = (int) Float.parseFloat(row[42]);
					int arrDelayMinutes = (int) Float.parseFloat(row[43]);
					if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
						return false;
					} else if (arrDelay < 0 && arrDelayMinutes != 0) {
						return false;
					}
					
					if (arrDelayMinutes >= 15 && ((int) Float.parseFloat(row[44])) != 1) {
						return false;
					}
				}
				
			} catch (NumberFormatException exception) {
				
				return false;
				
			}
			
			return true;
		}
		
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			String[] row = parse(value.toString(), 110);
			if (row != null && sanityTest(row) && checkYear(row, 2010, 2015)) {
				try {
					String carrierCode = row[8];
					String year = row[0];
					String averagePrice = row[109];
					String elapsedTime = row[51];
					String distance = row[54];
					
					context.write(new Text(carrierCode), 
							new Text(year + ":" + averagePrice + "\t" + elapsedTime + "\t" + distance));
				} catch (NumberFormatException exception) {
					// Do Nothing : Unable to parse float values
				}
			}
		}
		
	}
	
	static class R extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)  
			throws IOException, InterruptedException {

			boolean active = false;
			
			for (Text value : values) {
				String[] data =  value.toString().split(":");
				if (data[0].equals("2015")) {
					active = true;
					break;
				}
			}
			
			if (active) {
				for (Text value : values) {
					String[] data =  value.toString().split(":");
					if (!data[0].equals("2015")) {
						context.write(key, new Text(data[1]));
					}
				}
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJar("job.jar");
		job.setMapperClass(M.class);
		job.setReducerClass(R.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new LinearRegression(), args));
		} catch (Exception e) {
			
		}
	}

}
