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

package edu.umd.cloud9.collection.clue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;

import edu.umd.cloud9.collection.DocnoMapping;

public class ClueWarcDocnoMappingBuilder extends Configured implements Tool, DocnoMapping.Builder {
  private static final Logger LOG = Logger.getLogger(ClueWarcDocnoMappingBuilder.class);

  /**
   * Creates an instance of this tool.
   */
  public ClueWarcDocnoMappingBuilder() {}

  @Override
  public int build(Path src, Path dest, Configuration conf) throws IOException {
    super.setConf(conf);
    return run(new String[] {
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + src.toString(),
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + dest.toString() });
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws IOException {
    DocnoMapping.DefaultBuilderOptions options = DocnoMapping.BuilderUtils.parseDefaultOptions(args);
    if ( options == null) {
      return -1;
    }

    LOG.info("Tool name: " + ClueWarcDocnoMappingBuilder.class.getSimpleName());
    LOG.info(" - input path: " + options.collection);
    LOG.info(" - output file: " + options.docnoMapping);

    FileSystem fs = FileSystem.get(getConf());
    FSDataOutputStream out = fs.create(new Path(options.docnoMapping), true);
    final InputStream in = ClueWarcDocnoMapping.class.getResourceAsStream("docno.mapping");
    List<String> lines = CharStreams.readLines(CharStreams.newReaderSupplier(
        new InputSupplier<InputStream>() {
          @Override public InputStream getInput() throws IOException {
            return in;
          }
        }, Charsets.UTF_8));
    out.write((Joiner.on("\n").join(lines) + "\n").getBytes());
    out.close();
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + ClueWarcDocnoMappingBuilder.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new ClueWarcDocnoMappingBuilder(), args);
  }
}