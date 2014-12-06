package edu.umd.cloud9.io;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

public class FileMerger extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(FileMerger.class);

  public static final Random RANDOM_GENERATOR = new Random();
  public static final int DEFAULT_RANDOM_STRING_LENGTH = 20;

  public static final String PATH_INDICATOR = "path";
  public static final String INTEGER_INDICATOR = "int";

  public static final String HELP_OPTION = "help";

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";

  public static final String MAPPER_OPTION = "mapper";
  public static final String REDUCER_OPTION = "reducer";

  public static final String MERGE = "merge-tmp-dir";

  public static final String LOCAL_MERGE_OPTION = "localmerge";
  public static final boolean LOCAL_MERGE = false;
  public static final String DELETE_SOURCE_OPTION = "deletesource";
  public static final boolean DELETE_SOURCE = false;
  public static final String TEXT_FILE_INPUT_FORMAT = "textformat";
  public static final boolean TEXT_FILE_INPUT = false;

  public static final String FILE_CONTENT_DELIMITER = "";

  /**
   * Generate a random string of given length.
   */
  public static String generateRandomString(int length) {
    return new BigInteger(length * 4, RANDOM_GENERATOR).toString(32);
  }

  /**
   * Generate a random string of default length.
   */
  public static String generateRandomString() {
    return generateRandomString(DEFAULT_RANDOM_STRING_LENGTH);
  }

  /**
   * This method merges all files specified by the glob expression
   * <code>inputFiles<code>
   */
  public static Path mergeTextFiles(Configuration configuration, String inputFiles,
      String outputFile, int numberOfMappers, boolean deleteSource) throws IOException {
    // TODO: add in configuration

    if (numberOfMappers <= 0) {
      return mergeTextFiles(configuration, inputFiles, outputFile, deleteSource, false);
    } else {
      return mergeFilesDistribute(configuration, inputFiles, outputFile, numberOfMappers,
          LongWritable.class, Text.class, TextInputFormat.class, TextOutputFormat.class,
          deleteSource, false);
    }
  }

  public static Path mergeTextFiles(Configuration configuration, String inputFiles,
      String outputFile, int numberOfMappers, boolean deleteSource,
      boolean deleteDestinationFileIfExist) throws IOException {
    if (numberOfMappers <= 0) {
      return mergeTextFiles(configuration, inputFiles, outputFile, deleteSource, deleteDestinationFileIfExist);
    } else {
      return mergeFilesDistribute(configuration, inputFiles, outputFile, numberOfMappers,
          LongWritable.class, Text.class, TextInputFormat.class, TextOutputFormat.class,
          deleteSource, deleteDestinationFileIfExist);
    }
  }

  /**
   * @param inputFiles a glob expression of the files to be merged
   * @param outputFile a destination file path
   * @param deleteSource delete source files after merging
   * @return
   * @throws IOException
   */
  private static Path mergeTextFiles(Configuration configuration, String inputFiles,
      String outputFile, boolean deleteSource, boolean deleteDestinationFileIfExist)
      throws IOException {
    JobConf conf = new JobConf(configuration, FileMerger.class);
    FileSystem fs = FileSystem.get(conf);

    Path inputPath = new Path(inputFiles);
    Path outputPath = new Path(outputFile);

    if (deleteDestinationFileIfExist) {
      if (fs.exists(outputPath)) {
        // carefully remove the destination file, not recursive
        fs.delete(outputPath, false);
        sLogger.info("Warning: remove destination file since it already exists...");
      }
    } else {
      Preconditions.checkArgument(!fs.exists(outputPath), new IOException(
          "Destination file already exists..."));
    }

    FileUtil.copyMerge(fs, inputPath, fs, outputPath, deleteSource, conf, FILE_CONTENT_DELIMITER);
    sLogger.info("Successfully merge " + inputPath.toString() + " to " + outputFile);

    return outputPath;
  }

  public static Path mergeSequenceFiles(Configuration configuration, String inputFiles,
      String outputFile, int numberOfMappers, Class<? extends Writable> keyClass,
      Class<? extends Writable> valueClass, boolean deleteSource) throws IOException,
      InstantiationException, IllegalAccessException {
    if (numberOfMappers <= 0) {
      return mergeSequenceFiles(configuration, inputFiles, outputFile, keyClass, valueClass, deleteSource, false);
    } else {
      return mergeFilesDistribute(configuration, inputFiles, outputFile, numberOfMappers, keyClass,
          valueClass, SequenceFileInputFormat.class, SequenceFileOutputFormat.class, deleteSource,
          false);
    }
  }

  public static Path mergeSequenceFiles(Configuration configuration, String inputFiles,
      String outputFile, int numberOfMappers, Class<? extends Writable> keyClass,
      Class<? extends Writable> valueClass, boolean deleteSource,
      boolean deleteDestinationFileIfExist) throws IOException, InstantiationException,
      IllegalAccessException {
    if (numberOfMappers <= 0) {
      return mergeSequenceFiles(configuration, inputFiles, outputFile, keyClass, valueClass, deleteSource,
          deleteDestinationFileIfExist);
    } else {
      return mergeFilesDistribute(configuration, inputFiles, outputFile, numberOfMappers, keyClass,
          valueClass, SequenceFileInputFormat.class, SequenceFileOutputFormat.class, deleteSource,
          deleteDestinationFileIfExist);
    }
  }

  private static Path mergeSequenceFiles(Configuration configuration, String inputFiles,
      String outputFile, Class<? extends Writable> keyClass, Class<? extends Writable> valueClass,
      boolean deleteSource, boolean deleteDestinationFileIfExist) throws IOException,
      InstantiationException, IllegalAccessException {
    JobConf conf = new JobConf(configuration, FileMerger.class);
    FileSystem fs = FileSystem.get(conf);

    Path inputPath = new Path(inputFiles);
    Path outputPath = new Path(outputFile);

    if (deleteDestinationFileIfExist) {
      if (fs.exists(outputPath)) {
        // carefully remove the destination file, not recursive
        fs.delete(outputPath, false);
        sLogger.info("Warning: remove destination file since it already exists...");
      }
    } else {
      Preconditions.checkArgument(!fs.exists(outputPath), new IOException(
          "Destination file already exists..."));
    }

    FileStatus[] fileStatuses = fs.globStatus(inputPath);
    SequenceFile.Reader sequenceFileReader = null;
    SequenceFile.Writer sequenceFileWriter = null;

    Writable key, value;
    key = keyClass.newInstance();
    value = valueClass.newInstance();

    try {
      sequenceFileWriter = new SequenceFile.Writer(fs, conf, outputPath, keyClass, valueClass);

      for (FileStatus fileStatus : fileStatuses) {
        sLogger.info("Openning file " + fileStatus.getPath() + "...");
        sequenceFileReader = new SequenceFile.Reader(fs, fileStatus.getPath(), conf);

        while (sequenceFileReader.next(key, value)) {
          sequenceFileWriter.append(key, value);
        }

        if (deleteSource) {
          fs.deleteOnExit(fileStatus.getPath());
        }
      }
    } finally {
      IOUtils.closeStream(sequenceFileReader);
      IOUtils.closeStream(sequenceFileWriter);
    }

    sLogger.info("Successfully merge " + inputPath.toString() + " to " + outputFile);

    return outputPath;
  }

  private static Path mergeFilesDistribute(Configuration configuration, String inputFiles,
      String outputFile, int numberOfMappers, Class<? extends Writable> keyClass,
      Class<? extends Writable> valueClass, Class<? extends FileInputFormat> fileInputClass,
      Class<? extends FileOutputFormat> fileOutputClass, boolean deleteSource,
      boolean deleteDestinationFileIfExist) throws IOException {
    JobConf conf = new JobConf(configuration, FileMerger.class);
    conf.setJobName(FileMerger.class.getSimpleName());
    FileSystem fs = FileSystem.get(conf);

    sLogger.info("Tool: " + FileMerger.class.getSimpleName());

    sLogger.info(" - merge files from: " + inputFiles);
    sLogger.info(" - merge files to: " + outputFile);

    conf.setNumMapTasks(numberOfMappers);
    conf.setNumReduceTasks(1);

    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    conf.setMapOutputKeyClass(keyClass);
    conf.setMapOutputValueClass(valueClass);
    conf.setOutputKeyClass(keyClass);
    conf.setOutputValueClass(valueClass);

    conf.setInputFormat(fileInputClass);
    conf.setOutputFormat(fileOutputClass);

    Path inputPath = new Path(inputFiles);

    Path mergePath = new Path(inputPath.getParent().toString() + Path.SEPARATOR + MERGE
        + generateRandomString());
    Preconditions.checkArgument(!fs.exists(mergePath), new IOException(
        "Intermediate merge directory already exists..."));

    Path outputPath = new Path(outputFile);
    if (deleteDestinationFileIfExist) {
      if (fs.exists(outputPath)) {
        // carefully remove the destination file, not recursive
        fs.delete(outputPath, false);
        sLogger.info("Warning: remove destination file since it already exists...");
      }
    } else {
      Preconditions.checkArgument(!fs.exists(outputPath), new IOException(
          "Destination file already exists..."));
    }

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, mergePath);
    FileOutputFormat.setCompressOutput(conf, true);

    try {
      long startTime = System.currentTimeMillis();
      RunningJob job = JobClient.runJob(conf);
      sLogger.info("Merge Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");

      fs.rename(new Path(mergePath.toString() + Path.SEPARATOR + "part-00000"), outputPath);

      if (deleteSource) {
        for (FileStatus fileStatus : fs.globStatus(inputPath)) {
          fs.deleteOnExit(fileStatus.getPath());
        }
      }
    } finally {
      fs.delete(mergePath, true);
    }

    sLogger.info("Successfully merge " + inputFiles.toString() + " to " + outputFile);

    return outputPath;
  }

  @Override
  /**
   * TODO: add in hadoop configuration
   */
  public int run(String[] args) throws IOException {
    Options options = new Options();

    options.addOption(HELP_OPTION, false, "print the help message");
    options.addOption(OptionBuilder.withArgName(PATH_INDICATOR).hasArg()
        .withDescription("input file or directory").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName(PATH_INDICATOR).hasArg()
        .withDescription("output file").create(OUTPUT_OPTION));
    options
        .addOption(OptionBuilder
            .withArgName(INTEGER_INDICATOR)
            .hasArg()
            .withDescription(
                "number of mappers (default to 0 and hence local merge mode, set to positive value to enable cluster merge mode)")
            .create(MAPPER_OPTION));
    options.addOption(OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator()
        .withDescription("assign value for given property").create("D"));
    options.addOption(TEXT_FILE_INPUT_FORMAT, false, "input file in sequence format");
    options.addOption(DELETE_SOURCE_OPTION, false, "delete sources after merging");

    int mapperTasks = 0;
    boolean deleteSource = DELETE_SOURCE;
    boolean textFileFormat = TEXT_FILE_INPUT;

    String inputPath = "";
    String outputPath = "";

    GenericOptionsParser genericOptionsParser = new GenericOptionsParser(args);
    Configuration configuration = genericOptionsParser.getConfiguration();

    CommandLineParser parser = new GnuParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      CommandLine line = parser.parse(options, args);

      if (line.hasOption(HELP_OPTION)) {
        formatter.printHelp(FileMerger.class.getName(), options);
        System.exit(0);
      }

      if (line.hasOption(INPUT_OPTION)) {
        inputPath = line.getOptionValue(INPUT_OPTION);
      } else {
        throw new ParseException("Parsing failed due to " + INPUT_OPTION + " not initialized...");
      }

      if (line.hasOption(OUTPUT_OPTION)) {
        outputPath = line.getOptionValue(OUTPUT_OPTION);
      } else {
        throw new ParseException("Parsing failed due to " + OUTPUT_OPTION + " not initialized...");
      }

      if (line.hasOption(MAPPER_OPTION)) {
        mapperTasks = Integer.parseInt(line.getOptionValue(MAPPER_OPTION));
        if (mapperTasks <= 0) {
          sLogger.info("Warning: " + MAPPER_OPTION + " is not positive, merge in local model...");
          mapperTasks = 0;
        }
      }

      if (line.hasOption(DELETE_SOURCE_OPTION)) {
        deleteSource = true;
      }

      if (line.hasOption(TEXT_FILE_INPUT_FORMAT)) {
        textFileFormat = true;
      }
    } catch (ParseException pe) {
      System.err.println(pe.getMessage());
      formatter.printHelp(FileMerger.class.getName(), options);
      System.exit(0);
    } catch (NumberFormatException nfe) {
      System.err.println(nfe.getMessage());
      System.exit(0);
    }

    try {
      merge(configuration, inputPath, outputPath, mapperTasks, textFileFormat, deleteSource);
    } catch (InstantiationException ie) {
      ie.printStackTrace();
    } catch (IllegalAccessException iae) {
      iae.printStackTrace();
    }

    return 0;
  }

  @SuppressWarnings("unchecked")
  public static Path merge(Configuration configuration, String inputPath, String outputPath,
      int mapperTasks, boolean textFileFormat, boolean deleteSource) throws IOException,
      InstantiationException, IllegalAccessException {
    Class<? extends Writable> keyClass = LongWritable.class;
    Class<? extends Writable> valueClass = Text.class;

    FileSystem fs = FileSystem.get(new Configuration());
    if (!textFileFormat) {
      FileStatus[] fileStatus = fs.globStatus(new Path(inputPath));
      Preconditions.checkArgument(fileStatus.length > 0, "Invalid input path...");
      SequenceFile.Reader reader = new SequenceFile.Reader(fs,
          fileStatus[fileStatus.length - 1].getPath(), fs.getConf());
      try {
        keyClass = (Class<? extends Writable>) reader.getKeyClass();
        valueClass = (Class<? extends Writable>) reader.getValueClass();
        sLogger.info("Key type: " + keyClass.toString());
        sLogger.info("Value type: " + valueClass.toString());
      } catch (Exception e) {
        throw new RuntimeException("Error in loading key/value class");
      }
      reader.close();
    }

    if (textFileFormat) {
      return mergeTextFiles(configuration, inputPath, outputPath, mapperTasks, deleteSource);
    } else {
      return mergeSequenceFiles(configuration, inputPath, outputPath, mapperTasks, keyClass,
          valueClass, deleteSource);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FileMerger(), args);
    System.exit(res);
  }
}