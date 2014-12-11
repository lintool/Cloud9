/*
 * Cloud9: A MapReduce Library for Hadoop
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

package edu.umd.cloud9.collection;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * <p>
 * Interface for an object that maintains a bidirectional mapping between docids and docnos. A
 * docid is a globally-unique String identifier for a document in the collection. For many types of
 * information retrieval algorithms, documents in the collection must be sequentially numbered;
 * thus, each document in the collection must be assigned a unique integer identifier, which is its
 * docno. Typically, the docid/docno mappings are stored in a mappings file, which is loaded into
 * memory by concrete objects implementing this interface.
 * </p>
 *
 * <p>
 * Unless there are compelling reasons otherwise, it is preferable to start numbering docnos from
 * one instead of zero. This is because zero cannot be represented in many common compression
 * schemes that are used in information retrieval (e.g., Golomb codes).
 * </p>
 *
 * @author Jimmy Lin
 */
public interface DocnoMapping {
  /**
   * Returns the docno for a particular docid.
   *
   * @param docid the docid
   * @return the docno for the docid
   */
  int getDocno(String docid);

  /**
   * Returns the docid for a particular docno.
   *
   * @param docno the docno
   * @return the docid for the docno
   */
  String getDocid(int docno);

  /**
   * Loads a mapping file.
   *
   * @param path path to the mappings file
   * @param fs reference to the {@code FileSystem}
   * @throws IOException
   */
  void loadMapping(Path path, FileSystem fs) throws IOException;

  /**
   * Returns the builder for this mapping.
   *
   * @return builder for this mapping
   */
  Builder getBuilder();

  /**
   * Interface for an object that constructs a {@code DocnoMapping}.
   *
   * @author Jimmy Lin
   */
  public interface Builder {
    int build(Path src, Path dest, Configuration conf) throws IOException;
  }

  @SuppressWarnings("rawtypes")
  public static class DefaultBuilderOptions {
    public Class<? extends InputFormat> inputFormat;
    public String collection;
    public String docnoMapping;
  }

  @SuppressWarnings( { "static-access", "unchecked", "rawtypes" })
  public static class BuilderUtils {
    public static final String COLLECTION_OPTION = "collection";
    public static final String MAPPING_OPTION = "docnoMapping";
    public static final String FORMAT_OPTION = "inputFormat";

    public static DefaultBuilderOptions parseDefaultOptions(String[] args) {
      Options options = new Options();
      options.addOption(OptionBuilder.withArgName("path").hasArg()
          .withDescription("(required) collection path").create(COLLECTION_OPTION));
      options.addOption(OptionBuilder.withArgName("path").hasArg()
          .withDescription("(required) output DocnoMapping path").create(MAPPING_OPTION));
      options.addOption(OptionBuilder.withArgName("class").hasArg()
          .withDescription("(optional) fully-qualified Hadoop InputFormat").create(FORMAT_OPTION));

      CommandLine cmdline;
      CommandLineParser parser = new GnuParser();
      try {
        cmdline = parser.parse(options, args);
      } catch (ParseException exp) {
        System.err.println("Error parsing command line: " + exp.getMessage());
        return null;
      }

      if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(MAPPING_OPTION)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(DefaultBuilderOptions.class.getName(), options);
        ToolRunner.printGenericCommandUsage(System.out);
        return null;
      }

      DefaultBuilderOptions parsedOptions = new DefaultBuilderOptions();
      parsedOptions.inputFormat = SequenceFileInputFormat.class;
      if (cmdline.hasOption(FORMAT_OPTION)) {
        try {
          parsedOptions.inputFormat = (Class<? extends InputFormat>) Class.forName(cmdline
              .getOptionValue(FORMAT_OPTION));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      parsedOptions.collection = cmdline.getOptionValue(COLLECTION_OPTION);
      parsedOptions.docnoMapping = cmdline.getOptionValue(MAPPING_OPTION);

      return parsedOptions;
    }
  }
}
