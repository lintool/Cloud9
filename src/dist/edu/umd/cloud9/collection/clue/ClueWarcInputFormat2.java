/**
 * Hadoop FileInputFormat for reading WARC files
 *
 * (C) 2009 - Carnegie Mellon University
 *
 * 1. Redistributions of this source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The names "Lemur", "Indri", "University of Massachusetts",
 *    "Carnegie Mellon", and "lemurproject" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. To obtain permission, contact
 *    license@lemurproject.org.
 *
 * 4. Products derived from this software may not be called "Lemur" or "Indri"
 *    nor may "Lemur" or "Indri" appear in their names without prior written
 *    permission of The Lemur Project. To obtain permission,
 *    contact license@lemurproject.org.
 *
 * THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AS PART OF THE CLUEWEB09
 * PROJECT AND OTHER CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN
 * NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @author mhoy@cs.cmu.edu (Mark J. Hoy)
 */

package edu.umd.cloud9.collection.clue;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umd.cloud9.collection.generic.WebDocument;


public class ClueWarcInputFormat2 extends FileInputFormat<LongWritable, WebDocument> {

	/**
	 * Don't allow the files to be split!
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// ensure the input files are not splittable!
		return false;
	}

	/**
	 * Just return the record reader
	 */
	@Override
	public RecordReader<LongWritable, WebDocument> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new ClueWarcRecordReader(context.getConfiguration(), (FileSplit)split);
	}

	public static class ClueWarcRecordReader extends RecordReader<LongWritable, WebDocument> {
		private long mRecordCount = 1;
		private Path mFilePath = null;
		private DataInputStream mCompressedInput = null;

		private final LongWritable mCurKey = new LongWritable();
		private final ClueWarcRecord mCurValue = new ClueWarcRecord();

		public ClueWarcRecordReader(Configuration conf, FileSplit split) throws IOException {
			FileSystem fs = FileSystem.get(conf);
			mFilePath = split.getPath();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec compressionCodec = compressionCodecs.getCodec(mFilePath);

			mCompressedInput = new DataInputStream(compressionCodec.createInputStream(fs.open(mFilePath)));
		}

		@Override
		public void close() throws IOException {
			mCompressedInput.close();
		}

		@Override
		public float getProgress() throws IOException {
			return mRecordCount / 40000f;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return mCurKey;
		}

		@Override
		public WebDocument getCurrentValue() throws IOException, InterruptedException {
			return mCurValue;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			ClueWarcRecord newRecord = ClueWarcRecord.readNextWarcRecord(mCompressedInput);
			if (newRecord == null) {
				return false;
			}

			newRecord.setWarcFilePath(mFilePath.toString());

			mCurKey.set(mRecordCount);
			mCurValue.set(newRecord);

			mRecordCount++;
			return true;
		}
	}
}
