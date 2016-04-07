package edu.neu.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class DataInputBuffer extends DataInputStream {

	/** Constructs a new empty buffer. */
	public DataInputBuffer() {
		this(new Buffer());
	}

	private DataInputBuffer(Buffer buffer) {
		super(buffer);
		this.buffer = buffer;
	}

	private static class Buffer extends ByteArrayInputStream {
		public Buffer() {
			super(new byte[] {});
		}

		public void reset(byte[] input, int start, int length) {
			this.buf = input;
			this.count = start + length;
			this.mark = start;
			this.pos = start;
		}

		public byte[] getData() {
			return buf;
		}

		public int getPosition() {
			return pos;
		}

		public int getLength() {
			return count;
		}
	}
	
	private Buffer buffer;
	
	/** Resets the data that the buffer reads. */
	public void reset(byte[] input, int start, int length) {
		buffer.reset(input, start, length);
	}
	
}
