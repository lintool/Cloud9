package edu.umd.cloud9.util.map;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class FrontCodedString2IntBidiMapBuilder {
  private static final Logger LOG = Logger.getLogger(FrontCodedString2IntBidiMapBuilder.class);

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) ) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(FrontCodedString2IntBidiMapBuilder.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String input = cmdline.getOptionValue(INPUT);
    String output = cmdline.getOptionValue(OUTPUT);

    List<String> stringList = Lists.newArrayList();
    IntArrayList intList = new IntArrayList();

    // First read lines into sorted map to sort input.
    Object2IntAVLTreeMap<String> tree = new Object2IntAVLTreeMap<String>();
    BufferedReader br = new BufferedReader(new FileReader(input));
    String line;
    while ((line = br.readLine()) != null) {
      String[] arr = line.split("\\t");
      if ( arr[0] == null || arr[0].length() == 0) {
        LOG.info("Skipping invalid line: " + line);
      }
      tree.put(arr[0], Integer.parseInt(arr[1]));
    }
    br.close();

    // Extracted sorted strings and ints.
    for (Object2IntMap.Entry<String> map : tree.object2IntEntrySet()) {
      stringList.add(map.getKey());
      intList.add(map.getIntValue());
    }
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    FSDataOutputStream os = fs.create(new Path(output), true);

    ByteArrayOutputStream bytesOut;
    ObjectOutputStream objOut;
    byte[] bytes;

    // Serialize the front-coded dictionary
    FrontCodedStringList frontcodedList = new FrontCodedStringList(stringList, 8, true);

    bytesOut = new ByteArrayOutputStream();
    objOut = new ObjectOutputStream(bytesOut);
    objOut.writeObject(frontcodedList);
    objOut.close();

    bytes = bytesOut.toByteArray();
    os.writeInt(bytes.length);
    os.write(bytes);

    // Serialize the hash function
    ShiftAddXorSignedStringMap dict = new ShiftAddXorSignedStringMap(stringList.iterator(),
        new TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence>(stringList,
            TransformationStrategies.prefixFreeUtf16()));

    bytesOut = new ByteArrayOutputStream();
    objOut = new ObjectOutputStream(bytesOut);
    objOut.writeObject(dict);
    objOut.close();

    bytes = bytesOut.toByteArray();
    os.writeInt(bytes.length);
    os.write(bytes);

    // Serialize the ints.
    os.writeInt(intList.size());
    for (int i = 0; i < intList.size(); i++) {
      os.writeInt(intList.getInt(i));
    }
    
    os.close();
  }
}
