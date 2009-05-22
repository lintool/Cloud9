package edu.umd.cloud9.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Copied from LineRecordReader.LineReader in Hadoop 0.17.2 release.
 * 
 * 
 */
public class FSLineReader {
	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPosn = 0;

	/**
	 * Create a line reader that reads from the given stream using the given
	 * buffer-size.
	 * 
	 * @param in
	 * @throws IOException
	 */
	public FSLineReader(InputStream in, int bufferSize) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}

	public FSLineReader(Path path) throws IOException {
		Configuration config = new Configuration();
		FileSystem fileSys = FileSystem.get(config);

		this.in = fileSys.open(path);
		this.buffer = new byte[this.bufferSize];
	}

	public FSLineReader(String file) throws IOException {
		Configuration config = new Configuration();
		FileSystem fileSys = FileSystem.get(config);

		this.in = fileSys.open(new Path(file));
		this.buffer = new byte[this.bufferSize];
	}

	public FSLineReader(String file, FileSystem fs) throws IOException {
		this.in = fs.open(new Path(file));
		this.buffer = new byte[this.bufferSize];
	}

	public FSLineReader(Path file, FileSystem fs) throws IOException {
		this.in = fs.open(file);
		this.buffer = new byte[this.bufferSize];
	}

	/**
	 * Fill the buffer with more data.
	 * 
	 * @return was there more data?
	 * @throws IOException
	 */
	private boolean backfill() throws IOException {
		bufferPosn = 0;
		bufferLength = in.read(buffer);
		return bufferLength > 0;
	}

	/**
	 * Close the underlying stream.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		in.close();
	}

	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @return the number of bytes read including the newline
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readLine(Text str) throws IOException {
		str.clear();
		boolean hadFinalNewline = false;
		boolean hadFinalReturn = false;
		boolean hitEndOfFile = false;
		int startPosn = bufferPosn;
		outerLoop: while (true) {
			if (bufferPosn >= bufferLength) {
				if (!backfill()) {
					hitEndOfFile = true;
					break;
				}
			}
			startPosn = bufferPosn;
			for (; bufferPosn < bufferLength; ++bufferPosn) {
				switch (buffer[bufferPosn]) {
				case '\n':
					hadFinalNewline = true;
					bufferPosn += 1;
					break outerLoop;
				case '\r':
					if (hadFinalReturn) {
						// leave this \n in the stream, so we'll get it next
						// time
						break outerLoop;
					}
					hadFinalReturn = true;
					break;
				default:
					if (hadFinalReturn) {
						break outerLoop;
					}
				}
			}
			int length = bufferPosn - startPosn - (hadFinalReturn ? 1 : 0);
			if (length >= 0) {
				str.append(buffer, startPosn, length);
			}
		}
		int newlineLength = (hadFinalNewline ? 1 : 0) + (hadFinalReturn ? 1 : 0);
		if (!hitEndOfFile) {
			int length = bufferPosn - startPosn - newlineLength;
			if (length > 0) {
				str.append(buffer, startPosn, length);
			}
		}
		return str.getLength() + newlineLength;
	}

	public static void main(String[] args) throws Exception {

		FSLineReader reader = new FSLineReader(
				"../umd-hadoop-ivory-exp/qrels/genomics2005.topics.txt");

		Text t = new Text();
		while (reader.readLine(t) != 0) {
			System.out.println(t);
		}

		reader.close();

	}
}
