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

import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class TopNScoredObjects<K extends Comparable<K>> {
  private class ScoredObjectPriorityQueue extends
      PriorityQueue<PairOfObjectFloat<K>> {

    private ScoredObjectPriorityQueue(int maxSize) {
      super.initialize(maxSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean lessThan(Object obj0, Object obj1) {
      return ((PairOfObjectFloat<K>) obj0).getRightElement() < ((PairOfObjectFloat<K>) obj1)
          .getRightElement() ? true : false;
    }
  }

  private final ScoredObjectPriorityQueue queue;

  public TopNScoredObjects(int n) {
    queue = new ScoredObjectPriorityQueue(n);
  }

  public void add(K obj, float f) {
    queue.insert(new PairOfObjectFloat<K>(obj, f));
  }

  @SuppressWarnings("unchecked")
  public PairOfObjectFloat<K>[] extractAll() {
    int len = queue.size();
    PairOfObjectFloat<K>[] arr = (PairOfObjectFloat<K>[]) new PairOfObjectFloat[len];
    for (int i = 0; i < len; i++) {
      arr[len - 1 - i] = queue.pop();
    }
    return arr;
  }
}