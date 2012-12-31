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
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class AnalyzeBigramCount {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("usage: [input-path]");
      System.exit(-1);
    }

    System.out.println("input path: " + args[0]);
    List<PairOfWritables<Text, IntWritable>> bigrams =
        SequenceFileUtils.readDirectory(new Path(args[0]));

    Collections.sort(bigrams, new Comparator<PairOfWritables<Text, IntWritable>>() {
      public int compare(PairOfWritables<Text, IntWritable> e1,
          PairOfWritables<Text, IntWritable> e2) {
        if (e2.getRightElement().compareTo(e1.getRightElement()) == 0) {
          return e1.getLeftElement().compareTo(e2.getLeftElement());
        }

        return e2.getRightElement().compareTo(e1.getRightElement());
      }
    });

    int singletons = 0;
    int sum = 0;
    for (PairOfWritables<Text, IntWritable> bigram : bigrams) {
      sum += bigram.getRightElement().get();

      if (bigram.getRightElement().get() == 1) {
        singletons++;
      }
    }

    System.out.println("total number of unique bigrams: " + bigrams.size());
    System.out.println("total number of bigrams: " + sum);
    System.out.println("number of bigrams that appear only once: " + singletons);

    System.out.println("\nten most frequent bigrams: ");

    int cnt = 0;
    for (PairOfWritables<Text, IntWritable> bigram : bigrams) {
      System.out.println(bigram.getLeftElement() + "\t" + bigram.getRightElement());
      cnt++;

      if (cnt > 10) {
        break;
      }
    }
  }
}
