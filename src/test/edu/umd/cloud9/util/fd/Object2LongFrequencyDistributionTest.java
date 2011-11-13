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

package edu.umd.cloud9.util.fd;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.util.SortableEntries.Order;
import edu.umd.cloud9.util.pair.PairOfObjectLong;

public class Object2LongFrequencyDistributionTest {

  @Test
  public void test1Entry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    test1Common(fd);
  }

  @Test
  public void test1Fastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    test1Common(fd);
  }

  private void test1Common(Object2LongFrequencyDistribution<String> fd) {
    assertEquals(0, fd.get("a"));

    fd.increment("a");
    fd.increment("b");
    fd.increment("c");
    fd.increment("b");
    fd.increment("c");
    fd.increment("c");

    assertEquals(3, fd.getNumberOfEvents());
    assertEquals(6, fd.getSumOfCounts());

    assertEquals(1, fd.get("a"));
    assertEquals(2, fd.get("b"));
    assertEquals(3, fd.get("c"));

    assertEquals((float) 1 / 6, fd.getFrequency("a"), 10e-6);
    assertEquals((float) 2 / 6, fd.getFrequency("b"), 10e-6);
    assertEquals((float) 3 / 6, fd.getFrequency("c"), 10e-6);

    assertEquals(Math.log((float) 1 / 6), fd.getLogFrequency("a"), 10e-6);
    assertEquals(Math.log((float) 2 / 6), fd.getLogFrequency("b"), 10e-6);
    assertEquals(Math.log((float) 3 / 6), fd.getLogFrequency("c"), 10e-6);

    fd.decrement("c");

    assertEquals(3, fd.getNumberOfEvents());
    assertEquals(5, fd.getSumOfCounts());

    assertEquals(1, fd.get("a"));
    assertEquals(2, fd.get("b"));
    assertEquals(2, fd.get("c"));

    assertEquals((float) 1 / 5, fd.getFrequency("a"), 10e-6);
    assertEquals((float) 2 / 5, fd.getFrequency("b"), 10e-6);
    assertEquals((float) 2 / 5, fd.getFrequency("c"), 10e-6);

    assertEquals(Math.log((float) 1 / 5), fd.getLogFrequency("a"), 10e-6);
    assertEquals(Math.log((float) 2 / 5), fd.getLogFrequency("b"), 10e-6);
    assertEquals(Math.log((float) 2 / 5), fd.getLogFrequency("c"), 10e-6);

    fd.decrement("a");

    assertEquals(2, fd.getNumberOfEvents());
    assertEquals(4, fd.getSumOfCounts());

    assertEquals(0, fd.get("a"));
    assertEquals(2, fd.get("b"));
    assertEquals(2, fd.get("c"));

    assertEquals((float) 2 / 4, fd.getFrequency("b"), 10e-6);
    assertEquals((float) 2 / 4, fd.getFrequency("c"), 10e-6);

    assertEquals(Math.log((float) 2 / 4), fd.getLogFrequency("b"), 10e-6);
    assertEquals(Math.log((float) 2 / 4), fd.getLogFrequency("c"), 10e-6);
  }

  @Test
  public void test2Entry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    test2Common(fd);
  }

  @Test
  public void test2Fastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    test2Common(fd);
  }

  private void test2Common(Object2LongFrequencyDistribution<String> fd) {
    fd.increment("a");
    fd.increment("a");
    fd.increment("b");
    fd.increment("c");

    assertEquals(3, fd.getNumberOfEvents());
    assertEquals(4, fd.getSumOfCounts());

    assertEquals(2, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(1, fd.get("c"));

    fd.set("d", 5);

    assertEquals(4, fd.getNumberOfEvents());
    assertEquals(9, fd.getSumOfCounts());

    assertEquals(2, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(1, fd.get("c"));
    assertEquals(5, fd.get("d"));

    fd.set("a", 5);

    assertEquals(4, fd.getNumberOfEvents());
    assertEquals(12, fd.getSumOfCounts());

    assertEquals(5, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(1, fd.get("c"));
    assertEquals(5, fd.get("d"));

    fd.increment("c");
    fd.increment("c");
    fd.increment("c");

    assertEquals(4, fd.getNumberOfEvents());
    assertEquals(15, fd.getSumOfCounts());

    assertEquals(5, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(4, fd.get("c"));
    assertEquals(5, fd.get("d"));

    fd.set("c", 1);

    assertEquals(4, fd.getNumberOfEvents());
    assertEquals(12, fd.getSumOfCounts());

    assertEquals(5, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(1, fd.get("c"));
    assertEquals(5, fd.get("d"));
  }

  @Test
  public void test3Entry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    test3Common(fd);
  }

  @Test
  public void test3Fastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    test3Common(fd);
  }

  private void test3Common(Object2LongFrequencyDistribution<String> fd) {
    fd.increment("a");
    fd.increment("a");
    fd.increment("b");
    fd.increment("c");

    assertEquals(3, fd.getNumberOfEvents());
    assertEquals(4, fd.getSumOfCounts());

    assertEquals(2, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(1, fd.get("c"));

    fd.clear();
    assertEquals(0, fd.getNumberOfEvents());
    assertEquals(0, fd.getSumOfCounts());
  }

  @Test(expected = RuntimeException.class)
  public void testFailedDecrement1Entry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    testFailedDecrement1Common(fd);
  }

  @Test(expected = RuntimeException.class)
  public void testFailedDecrement1Fastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    testFailedDecrement1Common(fd);
  }

  private void testFailedDecrement1Common(
      Object2LongFrequencyDistribution<String> fd) {
    fd.increment("a");

    assertEquals(1, fd.getNumberOfEvents());
    assertEquals(1, fd.getSumOfCounts());
    assertEquals(1, fd.get("a"));

    fd.decrement("a");

    assertEquals(0, fd.getNumberOfEvents());
    assertEquals(0, fd.getSumOfCounts());
    assertEquals(0, fd.get("a"));

    fd.decrement("a");
  }

  @Test(expected = RuntimeException.class)
  public void testFailedDecrement2Entry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    testFailedDecrement2Common(fd);
  }

  @Test(expected = RuntimeException.class)
  public void testFailedDecrement2Fastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    testFailedDecrement2Common(fd);
  }

  private void testFailedDecrement2Common(
      Object2LongFrequencyDistribution<String> fd) {
    fd.increment("a", 1000);

    assertEquals(1, fd.getNumberOfEvents());
    assertEquals(1000, fd.getSumOfCounts());
    assertEquals(1000, fd.get("a"));

    fd.decrement("a", 997);

    assertEquals(1, fd.getNumberOfEvents());
    assertEquals(3, fd.getSumOfCounts());
    assertEquals(3, fd.get("a"));

    fd.decrement("a", 3);

    assertEquals(0, fd.getNumberOfEvents());
    assertEquals(0, fd.getSumOfCounts());
    assertEquals(0, fd.get("a"));

    fd.increment("a", 3);
    fd.decrement("a", 4);
  }

  @Test
  public void testMultiIncrementDecrementEntry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    testMultiIncrementDecrementCommon(fd);
  }

  @Test
  public void testMultiIncrementDecrementFastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    testMultiIncrementDecrementCommon(fd);
  }

  private void testMultiIncrementDecrementCommon(
      Object2LongFrequencyDistribution<String> fd) {
    fd.increment("a", 2);
    fd.increment("b", 3);
    fd.increment("c", 4);

    assertEquals(3, fd.getNumberOfEvents());
    assertEquals(9, fd.getSumOfCounts());

    assertEquals(2, fd.get("a"));
    assertEquals(3, fd.get("b"));
    assertEquals(4, fd.get("c"));

    fd.decrement("b", 2);

    assertEquals(3, fd.getNumberOfEvents());
    assertEquals(7, fd.getSumOfCounts());

    assertEquals(2, fd.get("a"));
    assertEquals(1, fd.get("b"));
    assertEquals(4, fd.get("c"));
  }

  @Test
  public void testGetFrequencySortedEntry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    testGetFrequencySortedCommon(fd);
  }

  @Test
  public void testGetFrequencySortedFastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    testGetFrequencySortedCommon(fd);
  }

  private void testGetFrequencySortedCommon(
      Object2LongFrequencyDistribution<String> fd) {
    fd.set("a", 5L);
    fd.set("d", 2L);
    fd.set("b", 5L);
    fd.set("e", 2L);
    fd.set("f", 1L);
    fd.set("c", 5L);

    assertEquals(6, fd.getNumberOfEvents());
    assertEquals(20, fd.getSumOfCounts());

    List<PairOfObjectLong<String>> list = fd
        .getEntries(Order.ByRightElementDescending);

    assertEquals(6, list.size());

    assertEquals("a", list.get(0).getLeftElement());
    assertEquals(5, list.get(0).getRightElement());
    assertEquals("b", list.get(1).getLeftElement());
    assertEquals(5, list.get(1).getRightElement());
    assertEquals("c", list.get(2).getLeftElement());
    assertEquals(5, list.get(2).getRightElement());
    assertEquals("d", list.get(3).getLeftElement());
    assertEquals(2, list.get(3).getRightElement());
    assertEquals("e", list.get(4).getLeftElement());
    assertEquals(2, list.get(4).getRightElement());
    assertEquals("f", list.get(5).getLeftElement());
    assertEquals(1, list.get(5).getRightElement());

    list = fd.getEntries(Order.ByRightElementAscending);

    assertEquals(6, list.size());

    assertEquals("f", list.get(0).getLeftElement());
    assertEquals(1, list.get(0).getRightElement());
    assertEquals("d", list.get(1).getLeftElement());
    assertEquals(2, list.get(1).getRightElement());
    assertEquals("e", list.get(2).getLeftElement());
    assertEquals(2, list.get(2).getRightElement());
    assertEquals("a", list.get(3).getLeftElement());
    assertEquals(5, list.get(3).getRightElement());
    assertEquals("b", list.get(4).getLeftElement());
    assertEquals(5, list.get(4).getRightElement());
    assertEquals("c", list.get(5).getLeftElement());
    assertEquals(5, list.get(5).getRightElement());

    list = fd.getEntries(Order.ByRightElementDescending, 4);

    assertEquals(4, list.size());

    assertEquals("a", list.get(0).getLeftElement());
    assertEquals(5, list.get(0).getRightElement());
    assertEquals("b", list.get(1).getLeftElement());
    assertEquals(5, list.get(1).getRightElement());
    assertEquals("c", list.get(2).getLeftElement());
    assertEquals(5, list.get(2).getRightElement());
    assertEquals("d", list.get(3).getLeftElement());
    assertEquals(2, list.get(3).getRightElement());

    list = fd.getEntries(Order.ByRightElementAscending, 4);

    assertEquals(4, list.size());

    assertEquals("f", list.get(0).getLeftElement());
    assertEquals(1, list.get(0).getRightElement());
    assertEquals("d", list.get(1).getLeftElement());
    assertEquals(2, list.get(1).getRightElement());
    assertEquals("e", list.get(2).getLeftElement());
    assertEquals(2, list.get(2).getRightElement());
    assertEquals("a", list.get(3).getLeftElement());
    assertEquals(5, list.get(3).getRightElement());
  }

  @Test
  public void testGetSortedEventsEntry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    testGetSortedEventsCommon(fd);
  }

  @Test
  public void testGetSortedEventsFastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    testGetSortedEventsCommon(fd);
  }

  private void testGetSortedEventsCommon(Object2LongFrequencyDistribution<String> fd) {
    fd.set("a", 1L);
    fd.set("d", 3L);
    fd.set("b", 4L);
    fd.set("e", 7L);
    fd.set("f", 9L);
    fd.set("c", 2L);

    assertEquals(6, fd.getNumberOfEvents());
    assertEquals(26, fd.getSumOfCounts());

    List<PairOfObjectLong<String>> list = fd.getEntries(Order.ByLeftElementAscending);

    assertEquals(6, list.size());

    assertEquals("a", list.get(0).getLeftElement());
    assertEquals(1, list.get(0).getRightElement());
    assertEquals("b", list.get(1).getLeftElement());
    assertEquals(4, list.get(1).getRightElement());
    assertEquals("c", list.get(2).getLeftElement());
    assertEquals(2, list.get(2).getRightElement());
    assertEquals("d", list.get(3).getLeftElement());
    assertEquals(3, list.get(3).getRightElement());
    assertEquals("e", list.get(4).getLeftElement());
    assertEquals(7, list.get(4).getRightElement());
    assertEquals("f", list.get(5).getLeftElement());
    assertEquals(9, list.get(5).getRightElement());

    list = fd.getEntries(Order.ByLeftElementDescending);

    assertEquals(6, list.size());

    assertEquals("f", list.get(0).getLeftElement());
    assertEquals(9, list.get(0).getRightElement());
    assertEquals("e", list.get(1).getLeftElement());
    assertEquals(7, list.get(1).getRightElement());
    assertEquals("d", list.get(2).getLeftElement());
    assertEquals(3, list.get(2).getRightElement());
    assertEquals("c", list.get(3).getLeftElement());
    assertEquals(2, list.get(3).getRightElement());
    assertEquals("b", list.get(4).getLeftElement());
    assertEquals(4, list.get(4).getRightElement());
    assertEquals("a", list.get(5).getLeftElement());
    assertEquals(1, list.get(5).getRightElement());

    list = fd.getEntries(Order.ByLeftElementAscending, 4);

    assertEquals(4, list.size());

    assertEquals("a", list.get(0).getLeftElement());
    assertEquals(1, list.get(0).getRightElement());
    assertEquals("b", list.get(1).getLeftElement());
    assertEquals(4, list.get(1).getRightElement());
    assertEquals("c", list.get(2).getLeftElement());
    assertEquals(2, list.get(2).getRightElement());
    assertEquals("d", list.get(3).getLeftElement());
    assertEquals(3, list.get(3).getRightElement());

    list = fd.getEntries(Order.ByLeftElementDescending, 4);

    assertEquals(4, list.size());

    assertEquals("f", list.get(0).getLeftElement());
    assertEquals(9, list.get(0).getRightElement());
    assertEquals("e", list.get(1).getLeftElement());
    assertEquals(7, list.get(1).getRightElement());
    assertEquals("d", list.get(2).getLeftElement());
    assertEquals(3, list.get(2).getRightElement());
    assertEquals("c", list.get(3).getLeftElement());
    assertEquals(2, list.get(3).getRightElement());
  }

  @Test
  public void testIterableEntry() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionEntry<String>();
    testIterableCommon(fd);
  }

  @Test
  public void testIterableFastutil() {
    Object2LongFrequencyDistribution<String> fd =
      new Object2LongFrequencyDistributionFastutil<String>();
    testIterableCommon(fd);
  }

  private void testIterableCommon(Object2LongFrequencyDistribution<String> fd) {
    fd.set("a", 1);
    fd.set("d", 3);
    fd.set("b", 4);
    fd.set("e", 7);
    fd.set("f", 9);
    fd.set("c", 2);

    assertEquals(6, fd.getNumberOfEvents());
    assertEquals(26, fd.getSumOfCounts());

    SortedSet<PairOfObjectLong<String>> list = new TreeSet<PairOfObjectLong<String>>();

    for (PairOfObjectLong<String> pair : fd) {
      list.add(pair.clone());
    }

    assertEquals(6, list.size());

    Iterator<PairOfObjectLong<String>> iter = list.iterator();
    PairOfObjectLong<String> e = iter.next();
    assertEquals("a", e.getLeftElement());
    assertEquals(1, e.getRightElement());
    e = iter.next();
    assertEquals("b", e.getLeftElement());
    assertEquals(4, e.getRightElement());
    e = iter.next();
    assertEquals("c", e.getLeftElement());
    assertEquals(2, e.getRightElement());
    e = iter.next();
    assertEquals("d", e.getLeftElement());
    assertEquals(3, e.getRightElement());
    e = iter.next();
    assertEquals("e", e.getLeftElement());
    assertEquals(7, e.getRightElement());
    e = iter.next();
    assertEquals("f", e.getLeftElement());
    assertEquals(9, e.getRightElement());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Object2LongFrequencyDistributionTest.class);
  }
}