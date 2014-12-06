package edu.umd.cloud9.util.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Sets;

public class TMapIVTest {

  @Test
  public void testBasic1() {
    TMapIV<String> map = new TMapIV<String>();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());

    map.put(2, "foo");
    assertFalse(map.isEmpty());

    map.clear();
    assertTrue(map.isEmpty());
    
    map.put(2, "foo");
    map.put(52, "bar");
    map.put(1, "baz");
    map.put(51, "a");
    map.put(99, "b");
    assertFalse(map.isEmpty());

    assertEquals("foo", map.get(2));
    assertEquals("bar", map.get(52));
    assertEquals("baz", map.get(1));
    assertEquals("a", map.get(51));
    assertEquals("b", map.get(99));

    Iterator<MapIV.Entry<String>> iter = map.entrySet().iterator();
    MapIV.Entry<String> entry;

    assertTrue(iter.hasNext());
    entry = iter.next();
    assertEquals(1, entry.getKey());
    assertEquals("baz", entry.getValue());

    assertTrue(iter.hasNext());
    entry = iter.next();
    assertEquals(2, entry.getKey());
    assertEquals("foo", entry.getValue());

    assertTrue(iter.hasNext());
    entry = iter.next();
    assertEquals(51, entry.getKey());
    assertEquals("a", entry.getValue());

    assertTrue(iter.hasNext());
    entry = iter.next();
    assertEquals(52, entry.getKey());
    assertEquals("bar", entry.getValue());

    assertTrue(iter.hasNext());
    entry = iter.next();
    assertEquals(99, entry.getKey());
    assertEquals("b", entry.getValue());
  }

  @Test
  public void testBasic2() {
    int size = 100000;
    Random r = new Random();
    TreeSet<Integer> ints = Sets.newTreeSet();

    TMapIV<String> map = new TMapIV<String>();
    for (int i = 0; i < size; i++) {
      int rand = r.nextInt(size);
      if ( ints.contains(rand)) {
        continue;
      }

      map.put(rand, rand + "");
      ints.add(rand);
    }

    for (Integer rand : ints) {
      String v = map.get(rand);

      assertEquals(v, rand + "");
      assertTrue(map.containsKey(rand));
    }

    Iterator<Integer> gIter = ints.iterator();
    for ( MapIV.Entry<String> entry : map.entrySet()) {
      assertTrue(gIter.hasNext());
      assertEquals((int) gIter.next(), entry.getKey());
    }
    assertFalse(gIter.hasNext());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TMapIVTest.class);
  }
}