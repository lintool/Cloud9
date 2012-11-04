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

package edu.umd.cloud9.io.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Benchmark for {@link Tuple}. See {@link BenchmarkPairOfInts} for more details.
 */
public class BenchmarkTuple {
  private static final int SAMPLES = 1000000;
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private BenchmarkTuple() {}

  /**
   * Runs this benchmark.
   */
  public static void main(String[] args) throws Exception {
    System.out.println("Number of samples: " + SAMPLES);
    Random r = new Random();

    long startTime;
    double duration;

    startTime = System.currentTimeMillis();

    List<Tuple> listTuples1 = new ArrayList<Tuple>();
    for (int i = 0; i < SAMPLES; i++) {
      Tuple tuple = TUPLE_FACTORY.newTuple();
      tuple.append(r.nextInt(1000));
      tuple.append(r.nextInt(1000));
      listTuples1.add(tuple);
    }

    duration = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Generated Tuples in " + duration + " seconds");

    startTime = System.currentTimeMillis();

    List<Tuple> listTuples2 = new ArrayList<Tuple>();
    for (Tuple t : listTuples1) {
      Tuple n = TUPLE_FACTORY.newTuple();
      n.append(t.get(0));
      n.append(t.get(1));
      listTuples2.add(n);
    }

    duration = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Cloned Tuples in " + duration + " seconds");

    startTime = System.currentTimeMillis();
    Collections.sort(listTuples2, new Comparator<Tuple>() {
      @Override
      public int compare(Tuple thisOne, Tuple thatOne) {
        int thisLeft;
        try {
          thisLeft = (Integer) thisOne.get(0);
          int thisRight = (Integer) thisOne.get(1);

          int thatLeft = (Integer) thatOne.get(0);
          int thatRight = (Integer) thatOne.get(1);

          if (thisLeft == thatLeft) {
            if (thisRight < thatRight) return -1;
            if (thisRight > thatRight) return 1;
            return 0;
          }

          if (thisLeft < thatLeft) return -1;
          if (thisLeft > thatLeft) return 1;

          return 0;
        } catch (ExecException e) {
          e.printStackTrace();
          return 0;
        }
      }
    });
    duration = (System.currentTimeMillis() - startTime) / 1000.0;

    System.out.println("Sorted Tuples in " + duration + " seconds");
  }
}
