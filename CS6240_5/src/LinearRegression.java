import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
 * MapReduce program to output the flight data for given year
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
			if (row != null && row.length != 110/* sanityTest(row)*/) {
				try {
					String carrierCode = row[8];
					String year = row[0];
					String dest_airID = row[20];
					String org_airID = row[11];

					String dep_date = row[5];
					String schedule_dep = row[29];
					String act_dep = row[30];
					String schedule_arr = row[40];
					String act_arr = row[41];
					
					if (act_dep.length() > 0 && act_arr.length() > 0) {
						context.write(new Text(carrierCode + " " +year + " " +dest_airID), 
								new Text("F" + ":" +dep_date + "\t" + schedule_arr + "\t" + act_arr));
						context.write(new Text(carrierCode + " " +year + " " +org_airID), 
								new Text("G" + ":" +dep_date + "\t" + schedule_dep + "\t" + act_dep));
					}
					// A:02-09-2013	sch_dep act_dep
				} catch (NumberFormatException exception) {
					// Do Nothing : Unable to parse float values
				}
			}
		}
		
	}
	
	static class R extends Reducer<Text, Text, Text, Text> {
		
		boolean timeDiff(Date arr, Date dep) {
			int time = ((24 - arr.getHours()) + dep.getHours())*60 + (dep.getMinutes() - arr.getMinutes()); 
			return time <= 6*60 && time >= 30; 
		}
		
		@SuppressWarnings("deprecation")
		boolean sameDay(Date arr, Date dep) {
			//if (order == "FG") {
				return (arr.getYear() == dep.getYear() && arr.getMonth() == dep.getMonth() 
					&& (arr.getDate() == dep.getDate() || 
					   ((dep.getDate() - arr.getDate() == 1) && timeDiff(arr, dep))));
			//}
		}
		
		boolean checkMissed(Date arr, Date dep) {
			int time = 0;
			if (dep.getDate() - arr.getDate() == 1) {
				time = ((24 - arr.getHours()) + dep.getHours())*60 + (dep.getMinutes() - arr.getMinutes());
			} else {
				time = (dep.getHours() - arr.getHours())*60 + (dep.getMinutes() - arr.getMinutes());
			}
			return time < 30;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)  
			throws IOException, InterruptedException {

			//boolean active = false;
			Iterable<Text> start = values;
			int total = 0;
			int missed = 0;
			
			
			for (Text value : values) {
				String[] arrData = value.toString().split(":");
				String[] arrDataval = arrData[1].split("\t");
				String arr_type = arrData[0];
				String pattern = "dd-MM-yyyy HHmm";
	    		SimpleDateFormat df = new SimpleDateFormat(pattern);
	    		Date arr_date = null;
				try {
					arr_date = df.parse(arrDataval[0] + " " +arrDataval[1]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
	    		Date actual_arr_date = null;
				try {
					actual_arr_date = df.parse(arrDataval[0] + " " +arrDataval[2]);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				if (arr_type.equals("F")) {
					for (Text v : start) {
						//continue;
						String[] depData = v.toString().split(":");
						String[] depDataval = depData[1].split("\t");
						String dep_type = depData[0];
						Date dep_date = null;
						try {
							dep_date = df.parse(depDataval[0] + " " +depDataval[1]);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						Date actual_dep_date = null;
						try {
							actual_dep_date = df.parse(depDataval[0] + " " +depDataval[2]);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						if (dep_type.equals("G")) {
							@SuppressWarnings("deprecation")
							//boolean connectingFlight = sameDay(arr_date, dep_date);
							boolean connectingFlight = true;
							if (connectingFlight) {
								total++;
								//boolean missedFlight = checkMissed(actual_arr_date, actual_dep_date);
								boolean missedFlight = true;
								if (missedFlight) {
									missed++;
								}
							}
						}
					}
				} else {
					for (Text v : start) {
						//continue;
						String[] depData = v.toString().split(":");
						String[] depDataval = depData[1].split("\t");
						//System.out.println(depDataval);
						String dep_type = depData[0];
						Date dep_date = null;
						try {
							dep_date = df.parse(depDataval[0] + " " +depDataval[1]);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						Date actual_dep_date = null;
						try {
							actual_dep_date = df.parse(depDataval[0] + " " +depDataval[2]);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						if (dep_type.equals("F")) {
							@SuppressWarnings("deprecation")
							//boolean connectingFlight = sameDay(arr_date, dep_date);
							boolean connectingFlight = true;
							if (connectingFlight) {
								total++;
								//boolean missedFlight = checkMissed(actual_arr_date, actual_dep_date);
								boolean missedFlight = true;
								if (missedFlight) {
									missed++;
								}
							}
						}
					}
				}
			}
			context.write(key, new Text(total + "\t" + missed));
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
