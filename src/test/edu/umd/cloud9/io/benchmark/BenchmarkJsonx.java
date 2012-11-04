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
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.io.JsonWritable;

/**
 * Benchmark for {@link JSONObjectWritable}. See {@link BenchmarkPairOfInts} for more details.
 */
public class BenchmarkJsonx {
  private static final int SAMPLES = 1000000;
  
  private BenchmarkJsonx() {}

  /**
   * Runs this benchmark.
   */
  private static class MyJsonWritable extends JsonWritable implements
      WritableComparable<MyJsonWritable> {
    public int compareTo(MyJsonWritable that) {
      int thisLeft = this.getJsonObject().get("left").getAsInt();
      int thisRight = this.getJsonObject().get("right").getAsInt();

      int thatLeft = that.getJsonObject().get("left").getAsInt();
      int thatRight = that.getJsonObject().get("right").getAsInt();

      if (thisLeft == thatLeft) {
        if (thisRight < thatRight)
          return -1;

        if (thisRight > thatRight)
          return 1;

        return 0;
      }

      if (thisLeft < thatLeft)
        return -1;

      if (thisLeft > thatLeft)
        return 1;

      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Number of samples: " + SAMPLES);
    Random r = new Random();

    long startTime;
    double duration;

    startTime = System.currentTimeMillis();
    List<MyJsonWritable> listJSONObjects1 = new ArrayList<MyJsonWritable>();
    for (int i = 0; i < SAMPLES; i++) {
      MyJsonWritable j = new MyJsonWritable();
      j.getJsonObject().addProperty("left", r.nextInt(1000));
      j.getJsonObject().addProperty("right", r.nextInt(1000));
      listJSONObjects1.add(j);
    }

    duration = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Generated JSON Objects in " + duration + " seconds");

    startTime = System.currentTimeMillis();
    List<MyJsonWritable> listJSONObjects2 = new ArrayList<MyJsonWritable>();
    for (MyJsonWritable t : listJSONObjects1) {
      MyJsonWritable n = new MyJsonWritable();
      n.getJsonObject().addProperty("left", t.getJsonObject().get("left").getAsInt());
      n.getJsonObject().addProperty("right", t.getJsonObject().get("right").getAsInt());
      listJSONObjects2.add(n);
    }

    duration = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Cloned " + SAMPLES + " JSON Objects in " + duration + " seconds");

    startTime = System.currentTimeMillis();
    Collections.sort(listJSONObjects2);
    duration = (System.currentTimeMillis() - startTime) / 1000.0;

    System.out.println("Sorted JSON Objects in " + duration + " seconds");
  }
}
