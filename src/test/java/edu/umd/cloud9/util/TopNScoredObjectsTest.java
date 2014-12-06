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

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class TopNScoredObjectsTest {

  @Test
  public void testBasic1() {
    TopNScoredObjects<String> set = new TopNScoredObjects<String>(5);

    set.add("1", 0);
    set.add("a", 5);
    set.add("b", 4);
    set.add("c", 6);
    set.add("d", 1);
    set.add("e", 199);
    set.add("2", -31);

    PairOfObjectFloat<String>[] arr = set.extractAll();

    assertEquals(5, arr.length);
    assertEquals("e", arr[0].getLeftElement());
    assertEquals(199, arr[0].getRightElement(), 10e-6);
    assertEquals("c", arr[1].getLeftElement());
    assertEquals(6, arr[1].getRightElement(), 10e-6);
    assertEquals("a", arr[2].getLeftElement());
    assertEquals(5, arr[2].getRightElement(), 10e-6);
    assertEquals("b", arr[3].getLeftElement());
    assertEquals(4, arr[3].getRightElement(), 10e-6);
    assertEquals("d", arr[4].getLeftElement());
    assertEquals(1, arr[4].getRightElement(), 10e-6);
  }

  @Test
  public void testBasic2() {
    TopNScoredObjects<String> set = new TopNScoredObjects<String>(5);

    set.add("1", 5);
    set.add("a", 5);
    set.add("b", 4);
    set.add("c", 6);
    set.add("d", 1);
    set.add("e", 1);
    // "e" should get preserved.

    PairOfObjectFloat<String>[] arr = set.extractAll();

    assertEquals(5, arr.length);
    assertEquals("c", arr[0].getLeftElement());
    assertEquals(6, arr[0].getRightElement(), 10e-6);
    assertEquals("1", arr[1].getLeftElement());
    assertEquals(5, arr[1].getRightElement(), 10e-6);
    assertEquals("a", arr[2].getLeftElement());
    assertEquals(5, arr[2].getRightElement(), 10e-6);
    assertEquals("b", arr[3].getLeftElement());
    assertEquals(4, arr[3].getRightElement(), 10e-6);
    assertEquals("e", arr[4].getLeftElement());
    assertEquals(1, arr[4].getRightElement(), 10e-6);
  }

  @Test
  public void testBasic3() {
    TopNScoredObjects<String> set = new TopNScoredObjects<String>(5);
    // What if # objects is less than size of queue?
    set.add("a", 5);
    set.add("b", 4);
    set.add("c", 6);

    PairOfObjectFloat<String>[] arr = set.extractAll();

    assertEquals(3, arr.length);
    assertEquals("c", arr[0].getLeftElement());
    assertEquals(6, arr[0].getRightElement(), 10e-6);
    assertEquals("a", arr[1].getLeftElement());
    assertEquals(5, arr[1].getRightElement(), 10e-6);
    assertEquals("b", arr[2].getLeftElement());
    assertEquals(4, arr[2].getRightElement(), 10e-6);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TopNScoredObjectsTest.class);
  }
}