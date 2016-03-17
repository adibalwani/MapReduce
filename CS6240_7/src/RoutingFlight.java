import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RoutingFlight implements Writable {

	// origin, Destination, carrier, flight_num, year, month, date, crs_dep_time, crs_arr_time
	private IntWritable arrFlightNumber;
	private IntWritable depFlightNumber;
	//private Text flightDate;
	private IntWritable dayOfMonth;
	private IntWritable dayOfWeek;
	private Text carrierCode;
	private IntWritable originAirportId;
	private IntWritable destAirportId;
	private IntWritable CRSDepTime;
	private IntWritable CRSArrTime;
	private BooleanWritable missedConnection;
	private BooleanWritable holiday;
	
	RoutingFlight() {
		arrFlightNumber = new IntWritable();
		depFlightNumber = new IntWritable();
		//flightDate = new Text();
		dayOfMonth = new IntWritable();
		dayOfWeek = new IntWritable();
		carrierCode = new Text();
		originAirportId = new IntWritable();
		destAirportId = new IntWritable();
		CRSDepTime = new IntWritable();
		CRSArrTime = new IntWritable();
		missedConnection = new BooleanWritable();
		holiday = new BooleanWritable();
	}
	
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		arrFlightNumber.readFields(dataInput);
		depFlightNumber.readFields(dataInput);
		//flightDate.readFields(dataInput);
		dayOfMonth.readFields(dataInput);
		dayOfWeek.readFields(dataInput);
		carrierCode.readFields(dataInput);
		originAirportId.readFields(dataInput);
		destAirportId.readFields(dataInput);
		CRSDepTime.readFields(dataInput);
		CRSArrTime.readFields(dataInput);
		missedConnection.readFields(dataInput);
		holiday.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		arrFlightNumber.write(dataOutput);
		depFlightNumber.write(dataOutput);
		//flightDate.write(dataOutput);
		dayOfMonth.write(dataOutput);
		dayOfWeek.write(dataOutput);
		carrierCode.write(dataOutput);
		originAirportId.write(dataOutput);
		destAirportId.write(dataOutput);
		CRSDepTime.write(dataOutput);
		CRSArrTime.write(dataOutput);
		missedConnection.write(dataOutput);
		holiday.write(dataOutput);
	}


	public IntWritable getArrFlightNumber() {
		return arrFlightNumber;
	}


	public void setArrFlightNumber(IntWritable arrFlightNumber) {
		this.arrFlightNumber = arrFlightNumber;
	}


	public IntWritable getDepFlightNumber() {
		return depFlightNumber;
	}


	public void setDepFlightNumber(IntWritable depFlightNumber) {
		this.depFlightNumber = depFlightNumber;
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


	public BooleanWritable getMissedConnection() {
		return missedConnection;
	}


	public void setMissedConnection(BooleanWritable missedConnection) {
		this.missedConnection = missedConnection;
	}


	public BooleanWritable getHoliday() {
		return holiday;
	}


	public void setHoliday(BooleanWritable holiday) {
		this.holiday = holiday;
	}

}
