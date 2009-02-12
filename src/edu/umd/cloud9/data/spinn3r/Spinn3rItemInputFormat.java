package edu.umd.cloud9.data.spinn3r;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.data.IndexableFileInputFormat;
import edu.umd.cloud9.data.XMLInputFormat;
import edu.umd.cloud9.data.XMLInputFormat.XMLRecordReader;

public class Spinn3rItemInputFormat extends IndexableFileInputFormat<LongWritable, Spinn3rItem> {

	public void configure(JobConf conf) {
	}

	public RecordReader<LongWritable, Spinn3rItem> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new Spinn3rItemRecordReader((FileSplit) inputSplit, conf);
	}

	public static class Spinn3rItemRecordReader implements RecordReader<LongWritable, Spinn3rItem> {
		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		static private long sOffset = 1000000000000000000L;

		private int mFileOffset;

		public Spinn3rItemRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, Spinn3rItem.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, Spinn3rItem.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);

			Path p = split.getPath();
			System.out.println("Current file: " + p.toString());
			FileSystem fs = p.getFileSystem(conf);

			FileStatus[] stats = fs.listStatus(p.getParent());
			for (int i = 0; i < stats.length; i++) {
				FileStatus s = stats[i];
				System.out.println("FileStatus: " + s.getPath().toString());
				if (s.getPath().equals(p)) {
					System.out.println("Matching!!" + i);
					mFileOffset = i;
				}
			}
		}

		public boolean next(LongWritable key, Spinn3rItem value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;

			Spinn3rItem.readItem(value, mText.toString());

			key.set(mFileOffset * sOffset + mLong.get());
			value.setDocid(key.toString());
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Spinn3rItem createValue() {
			return new Spinn3rItem();
		}

		public long getPos() throws IOException {
			return mReader.getPos();
		}

		public void close() throws IOException {
			mReader.close();
		}

		public float getProgress() throws IOException {
			return ((float) (mReader.getPos() - mReader.getStart()))
					/ ((float) (mReader.getEnd() - mReader.getStart()));
		}

	}

	/**
	 * Returns a 8-byte array built from a long.
	 * 
	 * @param n
	 *            The number to convert.
	 * @return A byte[].
	 */
	public static byte[] toBytes(long n) {
		return toBytes(n, new byte[8]);
	}

	/**
	 * Build a 8-byte array from a long. No check is performed on the array
	 * length.
	 * 
	 * @param n
	 *            The number to convert.
	 * @param b
	 *            The array to fill.
	 * @return A byte[].
	 */
	public static byte[] toBytes(long n, byte[] b) {
		b[7] = (byte) (n);
		n >>>= 8;
		b[6] = (byte) (n);
		n >>>= 8;
		b[5] = (byte) (n);
		n >>>= 8;
		b[4] = (byte) (n);
		n >>>= 8;
		b[3] = (byte) (n);
		n >>>= 8;
		b[2] = (byte) (n);
		n >>>= 8;
		b[1] = (byte) (n);
		n >>>= 8;
		b[0] = (byte) (n);

		return b;
	}

	/**
	 * Build a long from first 8 bytes of the array.
	 * 
	 * @param b
	 *            The byte[] to convert.
	 * @return A long.
	 */
	public static long toLong(byte[] b) {
		return ((((long) b[7]) & 0xFF) + ((((long) b[6]) & 0xFF) << 8)
				+ ((((long) b[5]) & 0xFF) << 16) + ((((long) b[4]) & 0xFF) << 24)
				+ ((((long) b[3]) & 0xFF) << 32) + ((((long) b[2]) & 0xFF) << 40)
				+ ((((long) b[1]) & 0xFF) << 48) + ((((long) b[0]) & 0xFF) << 56));
	}

}
