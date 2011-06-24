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

package edu.umd.cloud9.util.cfd;

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.fd.Int2LongFrequencyDistributionEntry;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.HMapIV;

/**
 * Implementation of {@link Int2IntConditionalFrequencyDistribution} based on
 * {@link HMapII}.
 *
 * @author Jimmy Lin
 *
 */
public class Int2IntConditionalFrequencyDistributionEntry implements Int2IntConditionalFrequencyDistribution {

  private final HMapIV<Int2IntFrequencyDistribution> distributions = new HMapIV<Int2IntFrequencyDistribution>();
  private final Int2LongFrequencyDistributionEntry marginals = new Int2LongFrequencyDistributionEntry();

  private long sumOfAllFrequencies = 0;

  @Override
  public void set(int k, int cond, int v) {
    if (!distributions.containsKey(cond)) {
      Int2IntFrequencyDistribution fd = new Int2IntFrequencyDistributionEntry();
      fd.set(k, v);
      distributions.put(cond, fd);
      marginals.increment(k, v);

      sumOfAllFrequencies += v;
    } else {
      Int2IntFrequencyDistribution fd = distributions.get(cond);
      int rv = fd.get(k);

      fd.set(k, v);
      distributions.put(cond, fd);
      marginals.increment(k, -rv + v);

      sumOfAllFrequencies = sumOfAllFrequencies - rv + v;
    }
  }

  @Override
  public void increment(int k, int cond) {
    increment(k, cond, 1);
  }

  @Override
  public void increment(int k, int cond, int v) {
    int cur = get(k, cond);
    if (cur == 0) {
      set(k, cond, v);
    } else {
      set(k, cond, cur + v);
    }
  }

  @Override
  public int get(int k, int cond) {
    if ( !distributions.containsKey(cond)) {
      return 0;
    }

    return distributions.get(cond).get(k);
  }

  @Override
  public long getMarginalCount(int k) {
    return marginals.get(k);
  }

  @Override
  public Int2IntFrequencyDistribution getConditionalDistribution(int cond) {
    if ( distributions.containsKey(cond) ) {
      return distributions.get(cond);
    }

    return new Int2IntFrequencyDistributionEntry();
  }

  @Override
  public long getSumOfAllCounts() {
    return sumOfAllFrequencies;
  }

  @Override
  public void check() {
    Int2IntFrequencyDistribution m = new Int2IntFrequencyDistributionEntry();

    long totalSum = 0;
    for (Int2IntFrequencyDistribution fd : distributions.values()) {
      long conditionalSum = 0;

      for (PairOfInts pair : fd) {
        conditionalSum += pair.getRightElement();
        m.increment(pair.getLeftElement(), pair.getRightElement());
      }

      if (conditionalSum != fd.getSumOfCounts()) {
        throw new RuntimeException("Internal Error!");
      }
      totalSum += fd.getSumOfCounts();
    }

    if (totalSum != getSumOfAllCounts()) {
      throw new RuntimeException("Internal Error! Got " + totalSum + ", Expected "  + getSumOfAllCounts());
    }

    for (PairOfInts e : m) {
      if ( e.getRightElement() != marginals.get(e.getLeftElement()) ) {
        throw new RuntimeException("Internal Error!");
      }
    }

    for (PairOfInts e : m) {
      if ( e.getRightElement() != m.get(e.getLeftElement()) ) {
        throw new RuntimeException("Internal Error!");
      }
    }
  }
}
