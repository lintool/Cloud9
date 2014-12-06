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

package edu.umd.cloud9.util;

import org.apache.hadoop.util.PriorityQueue;

import edu.umd.cloud9.io.pair.PairOfIntFloat;

public class TopNScoredInts {
  private class ScoredIntPriorityQueue extends
      PriorityQueue<PairOfIntFloat> {

    private ScoredIntPriorityQueue(int maxSize) {
      super.initialize(maxSize);
    }

    @Override
    protected boolean lessThan(Object obj0, Object obj1) {
      return ((PairOfIntFloat) obj0).getRightElement() < ((PairOfIntFloat) obj1).getRightElement() ?
          true : false;
    }
  }

  private final ScoredIntPriorityQueue queue;

  public TopNScoredInts(int n) {
    queue = new ScoredIntPriorityQueue(n);
  }

  public void add(int n, float f) {
    queue.insert(new PairOfIntFloat(n, f));
  }

  public PairOfIntFloat[] extractAll() {
    int len = queue.size();
    PairOfIntFloat[] arr = (PairOfIntFloat[]) new PairOfIntFloat[len];
    for (int i = 0; i < len; i++) {
      arr[len - 1 - i] = queue.pop();
    }
    return arr;
  }
}