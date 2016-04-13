package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

public class Text implements Writable,Comparable<Text> {

private	String value;


	public Text(String value) {
		super();
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		out.writeChars(value);
	}

	public void readFields(DataInput in) throws IOException {
		value=in.readLine();
	}
			  
	public int compareTo(Text o){
		return  o.getValue().compareTo(this.getValue());
	}

	public String getValue() {
		return value;
	}
}
