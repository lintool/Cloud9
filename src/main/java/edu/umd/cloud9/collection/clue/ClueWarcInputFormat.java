/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ClueWarcInputFormat extends FileInputFormat<LongWritable, ClueWarcRecord> {

  /**
   * Don't allow the files to be split!
   */
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    // ensure the input files are not splittable!
    return false;
  }

  /**
   * Just return the record reader
   */
  public RecordReader<LongWritable, ClueWarcRecord> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws IOException {
    return new ClueWarcRecordReader(conf, (FileSplit) split);
  }

  public static class ClueWarcRecordReader implements RecordReader<LongWritable, ClueWarcRecord> {
    private long recordCount = 1;
    private Path path = null;
    private DataInputStream input = null;

    private long totalNumBytesRead = 0;

    public ClueWarcRecordReader(Configuration conf, FileSplit split) throws IOException {
      FileSystem fs = FileSystem.get(conf);
      path = split.getPath();

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
      CompressionCodec compressionCodec = compressionCodecs.getCodec(path);
      input = new DataInputStream(compressionCodec.createInputStream(fs.open(path)));
    }

    @Override
    public boolean next(LongWritable key, ClueWarcRecord value) throws IOException {
      DataInputStream whichStream = input;

      ClueWarcRecord newRecord = ClueWarcRecord.readNextWarcRecord(whichStream);
      if (newRecord == null) {
        return false;
      }

      totalNumBytesRead += (long) newRecord.getTotalRecordLength();
      newRecord.setWarcFilePath(path.toString());

      value.set(newRecord);
      key.set(recordCount);

      recordCount++;
      return true;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }

    @Override
    public ClueWarcRecord createValue() {
      return new ClueWarcRecord();
    }

    @Override
    public long getPos() throws IOException {
      return totalNumBytesRead;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (float) recordCount / 40000f;
    }
  }
}
