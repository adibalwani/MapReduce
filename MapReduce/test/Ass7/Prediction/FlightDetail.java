import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.neu.hadoop.io.BooleanWritable;
import edu.neu.hadoop.io.IntWritable;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.io.Writable;
import edu.neu.hadoop.mapreduce.Context;


/**
 * Class representing flight details
 * 
 * @author Adib Alwani
 */
public class FlightDetail implements Writable {
	
	private IntWritable flightNumber;
	private Text flightDate;
	private IntWritable dayOfMonth;
	private IntWritable dayOfWeek;
	private Text carrierCode;
	private IntWritable originAirportId;
	private IntWritable destAirportId;
	private IntWritable CRSDepTime;
	private IntWritable CRSArrTime;
	private BooleanWritable delay;
	private BooleanWritable holiday;
	
	public FlightDetail() {
		flightNumber = new IntWritable();
		flightDate = new Text();
		dayOfMonth = new IntWritable();
		dayOfWeek = new IntWritable();
		carrierCode = new Text();
		originAirportId = new IntWritable();
		destAirportId = new IntWritable();
		CRSDepTime = new IntWritable();
		CRSArrTime = new IntWritable();
		delay = new BooleanWritable();
		holiday = new BooleanWritable();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		flightNumber.readFields(dataInput);
		flightDate.readFields(dataInput);
		dayOfMonth.readFields(dataInput);
		dayOfWeek.readFields(dataInput);
		carrierCode.readFields(dataInput);
		originAirportId.readFields(dataInput);
		destAirportId.readFields(dataInput);
		CRSDepTime.readFields(dataInput);
		CRSArrTime.readFields(dataInput);
		delay.readFields(dataInput);
		holiday.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		flightNumber.write(dataOutput);
		flightDate.write(dataOutput);
		dayOfMonth.write(dataOutput);
		dayOfWeek.write(dataOutput);
		carrierCode.write(dataOutput);
		originAirportId.write(dataOutput);
		destAirportId.write(dataOutput);
		CRSDepTime.write(dataOutput);
		CRSArrTime.write(dataOutput);
		delay.write(dataOutput);
		holiday.write(dataOutput);
	}

	public IntWritable getFlightNumber() {
		return flightNumber;
	}

	public void setFlightNumber(IntWritable flightNumber) {
		this.flightNumber = flightNumber;
	}

	public Text getFlightDate() {
		return flightDate;
	}

	public void setFlightDate(Text flightDate) {
		this.flightDate = flightDate;
	}

	public IntWritable getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(IntWritable dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public IntWritable getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(IntWritable dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public Text getCarrierCode() {
		return carrierCode;
	}

	public void setCarrierCode(Text carrierCode) {
		this.carrierCode = carrierCode;
	}

	public IntWritable getOriginAirportId() {
		return originAirportId;
	}

	public void setOriginAirportId(IntWritable originAirportId) {
		this.originAirportId = originAirportId;
	}

	public IntWritable getDestAirportId() {
		return destAirportId;
	}

	public void setDestAirportId(IntWritable destAirportId) {
		this.destAirportId = destAirportId;
	}

	public IntWritable getCRSDepTime() {
		return CRSDepTime;
	}

	public void setCRSDepTime(IntWritable cRSDepTime) {
		CRSDepTime = cRSDepTime;
	}

	public IntWritable getCRSArrTime() {
		return CRSArrTime;
	}

	public void setCRSArrTime(IntWritable cRSArrTime) {
		CRSArrTime = cRSArrTime;
	}

	public BooleanWritable getDelay() {
		return delay;
	}

	public void setDelay(BooleanWritable delay) {
		this.delay = delay;
	}

	public BooleanWritable getHoliday() {
		return holiday;
	}

	public void setHoliday(BooleanWritable holiday) {
		this.holiday = holiday;
	}
}
