import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
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
	private BooleanWritable arrival;
	private IntWritable dayOfMonth;
	private IntWritable dayOfWeek;
	private Text carrierCode;
	private Text origin;
	private Text destination;
	private IntWritable originAirportId;
	private IntWritable destAirportId;
	private LongWritable CRSDepTimeEpoch;
	private LongWritable CRSArrTimeEpoch;
	private IntWritable CRSDepTime;
	private IntWritable CRSArrTime;
	private BooleanWritable delay;
	private BooleanWritable holiday;
	
	public FlightDetail() {
		flightNumber = new IntWritable();
		arrival = new BooleanWritable();
		dayOfMonth = new IntWritable();
		dayOfWeek = new IntWritable();
		carrierCode = new Text();
		origin = new Text();
		destination = new Text();
		originAirportId = new IntWritable();
		destAirportId = new IntWritable();
		CRSDepTimeEpoch = new LongWritable();
		CRSArrTimeEpoch =  new LongWritable();
		CRSDepTime = new IntWritable();
		CRSArrTime = new IntWritable();
		delay = new BooleanWritable();
		holiday = new BooleanWritable();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		flightNumber.readFields(dataInput);
		arrival.readFields(dataInput);
		dayOfMonth.readFields(dataInput);
		dayOfWeek.readFields(dataInput);
		carrierCode.readFields(dataInput);
		origin.readFields(dataInput);
		destination.readFields(dataInput);
		originAirportId.readFields(dataInput);
		destAirportId.readFields(dataInput);
		CRSArrTimeEpoch.readFields(dataInput);
		CRSDepTimeEpoch.readFields(dataInput);
		CRSDepTime.readFields(dataInput);
		CRSArrTime.readFields(dataInput);
		delay.readFields(dataInput);
		holiday.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		flightNumber.write(dataOutput);
		arrival.write(dataOutput);
		dayOfMonth.write(dataOutput);
		dayOfWeek.write(dataOutput);
		carrierCode.write(dataOutput);
		origin.write(dataOutput);
		destination.write(dataOutput);
		originAirportId.write(dataOutput);
		destAirportId.write(dataOutput);
		CRSArrTimeEpoch.write(dataOutput);
		CRSDepTimeEpoch.write(dataOutput);
		CRSDepTime.write(dataOutput);
		CRSArrTime.write(dataOutput);
		delay.write(dataOutput);
		holiday.write(dataOutput);
	}
	
	/**
	 * Copy constructor - method
	 * 
	 * @param flightDetail Flight details to copy from
	 * @return Copied instance
	 */
	public FlightDetail copy(FlightDetail flightDetail) {
		FlightDetail flight = new FlightDetail();
		flight.setFlightNumber(new IntWritable(flightDetail.getFlightNumber().get()));
		flight.setArrival(new BooleanWritable(flightDetail.isArrival().get()));
		flight.setDayOfMonth(new IntWritable(flightDetail.getDayOfMonth().get()));
		flight.setDayOfWeek(new IntWritable(flightDetail.getDayOfWeek().get()));
		flight.setCarrierCode(new Text(flightDetail.getCarrierCode().toString()));
		flight.setOrigin(new Text(flightDetail.getOrigin().toString()));
		flight.setDestination(new Text(flightDetail.getDestination().toString()));
		flight.setOriginAirportId(new IntWritable(flightDetail.getOriginAirportId().get()));
		flight.setDestAirportId(new IntWritable(flightDetail.getDestAirportId().get()));
		flight.setCRSDepTimeEpoch(new LongWritable(flightDetail.getCRSDepTimeEpoch().get()));
		flight.setCRSArrTimeEpoch(new LongWritable(flightDetail.getCRSArrTimeEpoch().get()));
		flight.setCRSDepTime(new IntWritable(flightDetail.getCRSDepTime().get()));
		flight.setCRSArrTime(new IntWritable(flightDetail.getCRSArrTime().get()));
		flight.setDelay(new BooleanWritable(flightDetail.getDelay().get()));
		flight.setHoliday(new BooleanWritable(flightDetail.getHoliday().get()));
		return flight;
	}

	public IntWritable getFlightNumber() {
		return flightNumber;
	}

	public void setFlightNumber(IntWritable flightNumber) {
		this.flightNumber = flightNumber;
	}

	public BooleanWritable isArrival() {
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

	public LongWritable getCRSDepTimeEpoch() {
		return CRSDepTimeEpoch;
	}

	public void setCRSDepTimeEpoch(LongWritable crsDepTimeEpoch) {
		this.CRSDepTimeEpoch = crsDepTimeEpoch;
	}

	public LongWritable getCRSArrTimeEpoch() {
		return CRSArrTimeEpoch;
	}

	public void setCRSArrTimeEpoch(LongWritable crsArrTimeEpoch) {
		this.CRSArrTimeEpoch = crsArrTimeEpoch;
	}

	public IntWritable getCRSDepTime() {
		return CRSDepTime;
	}

	public void setCRSDepTime(IntWritable CRSDepTime) {
		this.CRSDepTime = CRSDepTime;
	}

	public IntWritable getCRSArrTime() {
		return CRSArrTime;
	}

	public void setCRSArrTime(IntWritable CRSArrTime) {
		this.CRSArrTime = CRSArrTime;
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
