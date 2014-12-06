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

package edu.umd.cloud9.util.pair;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.pair.PairOfObjectDouble;

public class PairOfObjectDoubleTest {

  @Test
  public void test1() {
    PairOfObjectDouble<String> pair = new PairOfObjectDouble<String>("a", 1.0);
    assertEquals("a", pair.getLeftElement());
    assertEquals(1.0, pair.getRightElement(), 10e-6);

    pair.setLeftElement("b");
    assertEquals("b", pair.getLeftElement());

    pair.setRightElement(5.0);
    assertEquals(5.0, pair.getRightElement(), 10e-6);

    pair.set("foo", -1.0);
    assertEquals("foo", pair.getLeftElement());
    assertEquals(-1.0, pair.getRightElement(), 10e-6);
  }

  @Test
  public void testIterable() {
    List<PairOfObjectDouble<String>> list = Lists.newArrayList();

    list.add(new PairOfObjectDouble<String>("f", 9.0));
    list.add(new PairOfObjectDouble<String>("a", 1.0));
    list.add(new PairOfObjectDouble<String>("b", 4.0));
    list.add(new PairOfObjectDouble<String>("b", 2.0));
    list.add(new PairOfObjectDouble<String>("c", 2.0));
    list.add(new PairOfObjectDouble<String>("a", 3.0));

    assertEquals(6, list.size());
    Collections.sort(list);

    assertEquals("a", list.get(0).getLeftElement());
    assertEquals(1.0, list.get(0).getRightElement(), 10e-6);
    assertEquals("a", list.get(1).getLeftElement());
    assertEquals(3.0, list.get(1).getRightElement(), 10e-6);
    assertEquals("b", list.get(2).getLeftElement());
    assertEquals(2.0, list.get(2).getRightElement(), 10e-6);
    assertEquals("b", list.get(3).getLeftElement());
    assertEquals(4.0, list.get(3).getRightElement(), 10e-6);
    assertEquals("c", list.get(4).getLeftElement());
    assertEquals(2.0, list.get(4).getRightElement(), 10e-6);
    assertEquals("f", list.get(5).getLeftElement());
    assertEquals(9.0, list.get(5).getRightElement(), 10e-6);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PairOfObjectDoubleTest.class);
  }
}
