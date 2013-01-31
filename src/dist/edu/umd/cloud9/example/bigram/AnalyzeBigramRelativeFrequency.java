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

package edu.umd.cloud9.example.bigram;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.util.ToolRunner;

import cern.colt.Arrays;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class AnalyzeBigramRelativeFrequency {
  private static final String INPUT = "input";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(AnalyzeBigramRelativeFrequency.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    System.out.println("input path: " + inputPath);

    List<PairOfWritables<PairOfStrings, FloatWritable>> pairs =
        SequenceFileUtils.readDirectory(new Path(inputPath));

    List<PairOfWritables<PairOfStrings, FloatWritable>> list1 = Lists.newArrayList();
    List<PairOfWritables<PairOfStrings, FloatWritable>> list2 = Lists.newArrayList();

    for (PairOfWritables<PairOfStrings, FloatWritable> p : pairs) {
      PairOfStrings bigram = p.getLeftElement();

      if (bigram.getLeftElement().equals("light")) {
        list1.add(p);
      }
      if (bigram.getLeftElement().equals("contain")) {
        list2.add(p);
      }
    }

    Collections.sort(list1, new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
      public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
          PairOfWritables<PairOfStrings, FloatWritable> e2) {
        if (e1.getRightElement().compareTo(e2.getRightElement()) == 0) {
          return e1.getLeftElement().compareTo(e2.getLeftElement());
        }

        return e2.getRightElement().compareTo(e1.getRightElement());
      }
    });

    Iterator<PairOfWritables<PairOfStrings, FloatWritable>> iter1 =
        Iterators.limit(list1.iterator(), 10);
    while (iter1.hasNext()) {
      PairOfWritables<PairOfStrings, FloatWritable> p = iter1.next();
      PairOfStrings bigram = p.getLeftElement();
      System.out.println(bigram + "\t" + p.getRightElement());
    }

    Collections.sort(list2, new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
      public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
          PairOfWritables<PairOfStrings, FloatWritable> e2) {
        if (e1.getRightElement().compareTo(e2.getRightElement()) == 0) {
          return e1.getLeftElement().compareTo(e2.getLeftElement());
        }

        return e2.getRightElement().compareTo(e1.getRightElement());
      }
    });

    Iterator<PairOfWritables<PairOfStrings, FloatWritable>> iter2 =
        Iterators.limit(list2.iterator(), 10);
    while (iter2.hasNext()) {
      PairOfWritables<PairOfStrings, FloatWritable> p = iter2.next();
      PairOfStrings bigram = p.getLeftElement();
      System.out.println(bigram + "\t" + p.getRightElement());
    }
  }
}
