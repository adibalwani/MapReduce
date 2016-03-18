import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/**
 * Class representing flight details
 * 
 * @author Adib Alwani
 */
public class FlightDetail implements Writable {
	
	private IntWritable flightNumber;
	private Text flightDate;
	private BooleanWritable arrival;
	private IntWritable dayOfMonth;
	private IntWritable dayOfWeek;
	private Text carrierCode;
	private Text origin;
	private Text destination;
	private IntWritable originAirportId;
	private IntWritable destAirportId;
	private LongWritable crsDepTimeEpoch;
	private LongWritable crsArrTimeEpoch;
	private IntWritable CRSDepTime;
	private IntWritable CRSArrTime;
	private BooleanWritable delay;
	private BooleanWritable holiday;
	
	public FlightDetail() {
		flightNumber = new IntWritable();
		flightDate = new Text();
		arrival = new BooleanWritable();
		dayOfMonth = new IntWritable();
		dayOfWeek = new IntWritable();
		carrierCode = new Text();
		origin = new Text();
		destination = new Text();
		originAirportId = new IntWritable();
		destAirportId = new IntWritable();
		crsDepTimeEpoch = new LongWritable();
		crsArrTimeEpoch =  new LongWritable();
		CRSDepTime = new IntWritable();
		CRSArrTime = new IntWritable();
		delay = new BooleanWritable();
		holiday = new BooleanWritable();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		flightNumber.readFields(dataInput);
		flightDate.readFields(dataInput);
		arrival.readFields(dataInput);
		dayOfMonth.readFields(dataInput);
		dayOfWeek.readFields(dataInput);
		carrierCode.readFields(dataInput);
		origin.readFields(dataInput);
		destination.readFields(dataInput);
		originAirportId.readFields(dataInput);
		destAirportId.readFields(dataInput);
		crsArrTimeEpoch.readFields(dataInput);
		crsDepTimeEpoch.readFields(dataInput);
		CRSDepTime.readFields(dataInput);
		CRSArrTime.readFields(dataInput);
		delay.readFields(dataInput);
		holiday.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		flightNumber.write(dataOutput);
		flightDate.write(dataOutput);
		arrival.write(dataOutput);
		dayOfMonth.write(dataOutput);
		dayOfWeek.write(dataOutput);
		carrierCode.write(dataOutput);
		origin.write(dataOutput);
		destination.write(dataOutput);
		originAirportId.write(dataOutput);
		destAirportId.write(dataOutput);
		crsArrTimeEpoch.write(dataOutput);
		crsDepTimeEpoch.write(dataOutput);
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

	public BooleanWritable getArrival() {
		return arrival;
	}

	public void setArrival(BooleanWritable arrival) {
		this.arrival = arrival;
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

	public Text getOrigin() {
		return origin;
	}

	public void setOrigin(Text source) {
		this.origin = source;
	}

	public Text getDestination() {
		return destination;
	}

	public void setDestination(Text destination) {
		this.destination = destination;
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

	public LongWritable getCrsDepTimeEpoch() {
		return crsDepTimeEpoch;
	}

	public void setCrsDepTimeEpoch(LongWritable crsDepTimeEpoch) {
		this.crsDepTimeEpoch = crsDepTimeEpoch;
	}

	public LongWritable getCrsArrTimeEpoch() {
		return crsArrTimeEpoch;
	}

	public void setCrsArrTimeEpoch(LongWritable crsArrTimeEpoch) {
		this.crsArrTimeEpoch = crsArrTimeEpoch;
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
