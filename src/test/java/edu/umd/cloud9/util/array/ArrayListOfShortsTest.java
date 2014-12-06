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

package edu.umd.cloud9.util.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Random;
import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import edu.umd.cloud9.util.array.ArrayListOfShorts;

public class ArrayListOfShortsTest {
  short neg_one=-1, zero=0, one=1, two=2, three=3, four=4, five=5, six=6, seven=7, nine=9;

  @Test
  public void testRemoveWithinBounds(){
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add(one).add(three).add(five).add(seven);
    
    assertTrue(one == a.remove(0));

    assertTrue(three == a.get(0));
    assertTrue(five == a.get(1));
    
    assertTrue(five == a.remove(1));
    assertTrue(seven == a.get(2));
  }
  
  @Test (expected=ArrayIndexOutOfBoundsException.class)
  public void testRemoveOutOfBounds(){
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add(one).add(three).add(five).add(seven);

    a.remove(4);
  }

  @Test (expected=ArrayIndexOutOfBoundsException.class)
  public void testRemoveOutOfBounds2(){
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add(neg_one);
    a.remove(-1);
  }
  
	@Test
	public void testBasic1() {
		int size = 10000;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(shorts[i], v);
		}
	}

  @Test
  public void testArrayConstructor() {
    short[] arr = new short[] { 1, 2, 3, 4, 5 };
    assertEquals(5, arr.length);

    ArrayListOfShorts list = new ArrayListOfShorts(arr);
    list.remove(2);

    // Make sure the original array remains untouched.
    assertEquals(1, arr[0]);
    assertEquals(2, arr[1]);
    assertEquals(3, arr[2]);
    assertEquals(4, arr[3]);
    assertEquals(5, arr[4]);
  }

	@Test
	public void testRemove() {
		ArrayListOfShorts list = new ArrayListOfShorts();
		for ( int i=0; i<10; i++) {
			list.add((short) i);
		}

		list.remove(list.indexOf((short) 5));
		assertEquals(9, list.size());
		assertEquals(0, list.get(0));
		assertEquals(1, list.get(1));
		assertEquals(2, list.get(2));
		assertEquals(3, list.get(3));
		assertEquals(4, list.get(4));
		assertEquals(6, list.get(5));
		assertEquals(7, list.get(6));
		assertEquals(8, list.get(7));
		assertEquals(9, list.get(8));

		list.remove(list.indexOf((short) 9));
		assertEquals(8, list.size);
		assertEquals(0, list.get(0));
		assertEquals(1, list.get(1));
		assertEquals(2, list.get(2));
		assertEquals(3, list.get(3));
		assertEquals(4, list.get(4));
		assertEquals(6, list.get(5));
		assertEquals(7, list.get(6));
		assertEquals(8, list.get(7));
	}

	@Test
	public void testUpdate() {
		int size = 10000;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			list.set(i, (short) (shorts[i] + 1));
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(shorts[i] + 1, v);
		}

	}

	@Test
	public void testTrim1() {
		int size = 89;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(shorts[i], v);
		}

		short[] rawArray = list.getArray();
		int lenBefore = rawArray.length;

		list.trimToSize();
		short[] rawArrayAfter = list.getArray();
		int lenAfter = rawArrayAfter.length;

		assertEquals(89, lenAfter);
		assertTrue(lenBefore > lenAfter);
	}

	@Test
	public void testClone() {
		int size = 10000;
		Random r = new Random();
		int[] shorts = new int[size];

		ArrayListOfShorts list1 = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list1.add(k);
			shorts[i] = k;
		}

		ArrayListOfShorts list2 = list1.clone();

		assertEquals(size, list1.size());
		assertEquals(size, list2.size());

		for (int i = 0; i < size; i++) {
			list2.set(i, (short) (shorts[i] + 1));
		}

		// values in old list should not have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(shorts[i], list1.get(i));
		}

		// however, values in new list should have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(shorts[i] + 1, list2.get(i));
		}
	}

  @Test
  public void testToString1() {
    assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",
        new ArrayListOfShorts((short) 1, (short) 11).toString());
    assertEquals("[1, 2, 3, 4, 5 ... (5 more) ]",
        new ArrayListOfShorts((short) 1, (short) 11).toString(5));

    assertEquals("[1, 2, 3, 4, 5]",
        new ArrayListOfShorts((short) 1, (short) 6).toString());
    assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]",
        new ArrayListOfShorts((short) 1, (short) 12).toString(11));

    assertEquals("[]", new ArrayListOfShorts().toString());
  }

	@Test
	public void testToString2() {
		int size = 10;
		Random r = new Random();

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
		    list.add((short) r.nextInt(Short.MAX_VALUE + 1));
		}

		String out = list.toString();
		for (int i = 0; i < size; i++) {
			short v = list.get(i);

			// Make sure the first 10 elements are printed out.
			assertTrue(out.indexOf(new Short(v).toString()) != -1);
		}

		for (int i = 0; i < size; i++) {
		    list.add((short) r.nextInt(Short.MAX_VALUE + 1));
		}

		out = list.toString();
		for (int i = size; i < size+size; i++) {
			short v = list.get(i);

			// Make sure these elements are not printed out.
			assertTrue(out.indexOf(new Short(v).toString()) == -1);
		}

		assertTrue(out.indexOf(size + " more") != -1);
	}

	@Test
	public void testIterable() {
		int size = 1000;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		int i=0;
		for ( Short v : list) {
			assertEquals(shorts[i++], (short) v);
		}

	}

	@Test
	public void testSetSize() {
		ArrayListOfShorts list = new ArrayListOfShorts();

		list.add((short) 5);
		assertEquals(1, list.size);
		assertEquals(5, list.get(0));

		list.setSize(5);
		assertEquals(5, list.size);
		assertEquals(0, list.get(1));
		assertEquals(0, list.get(2));
		assertEquals(0, list.get(3));
		assertEquals(0, list.get(4));

		list.add((short) 12);
		assertEquals(6, list.size);
		assertEquals(12, list.get(5));
	}
  @Test
  public void testSort() {
    ArrayListOfShorts a = new ArrayListOfShorts();
    assertEquals(0, a.size());

    a.add((short) 5);
    a.add((short) 6);
    a.add((short) 1);
    a.add((short) 4);
    assertEquals(4, a.size());

    a.sort();
    assertEquals(4, a.size());

    assertEquals(1, a.get(0));
    assertEquals(4, a.get(1));
    assertEquals(5, a.get(2));
    assertEquals(6, a.get(3));
  }

  @Test
  public void testIntersection1() {
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 5);
    a.add((short) 3);
    a.add((short) 1);

    a.sort();

    ArrayListOfShorts b = new ArrayListOfShorts();
    b.add((short) 0);
    b.add((short) 1);
    b.add((short) 2);
    b.add((short) 3);

    ArrayListOfShorts c = a.intersection(b);

    assertEquals(1, c.get(0));
    assertEquals(3, c.get(1));
    assertEquals(2, c.size());
  }

  @Test
  public void testIntersection2() {
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 5);

    ArrayListOfShorts b = new ArrayListOfShorts();
    b.add((short) 0);
    b.add((short) 1);
    b.add((short) 2);
    b.add((short) 3);

    ArrayListOfShorts c = a.intersection(b);
    assertTrue(c.size() == 0);
  }

  @Test
  public void testIntersection3() {
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 3);
    a.add((short) 5);
    a.add((short) 7);
    a.add((short) 8);
    a.add((short) 9);

    ArrayListOfShorts b = new ArrayListOfShorts();
    b.add((short) 0);
    b.add((short) 1);
    b.add((short) 2);
    b.add((short) 3);

    ArrayListOfShorts c = a.intersection(b);

    assertEquals(3, c.get(0));
    assertEquals(1, c.size());
  }

  @Test
  public void testIntersection4() {
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 3);

    ArrayListOfShorts b = new ArrayListOfShorts();
    b.add((short) 0);

    ArrayListOfShorts c = a.intersection(b);

    assertEquals(0, c.size());
  }
  
  @Test
  public void testMerge1() {
    //CASE: interleaved

    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 3);
    a.add((short) 7);
    a.add((short) 10);

    ArrayListOfShorts b = new ArrayListOfShorts();
    b.add((short) 0);
    b.add((short) 4);
    b.add((short) 9);

    ArrayListOfShorts c = a.merge(b);

    assertEquals(6, c.size());
    assertEquals(0, c.get(0));
    assertEquals(3, c.get(1));
    assertEquals(4, c.get(2));
    assertEquals(7, c.get(3));
    assertEquals(9, c.get(4));
    assertEquals(10, c.get(5));

    // c should be same as c2
    ArrayListOfShorts c2 = b.merge(a); 
    assertEquals(c, c2);
  }

  @Test
  public void testMerge2() {
    //CASE: append

    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 3);
    a.add((short) 7);
    a.add((short) 10);

    ArrayListOfShorts b = new ArrayListOfShorts();
    b.add((short) 11);
    b.add((short) 19);
    b.add((short) 21);

    ArrayListOfShorts c = a.merge(b);

    assertEquals(6, c.size());
    assertEquals(3, c.get(0));
    assertEquals(7, c.get(1));
    assertEquals(10, c.get(2));
    assertEquals(11, c.get(3));
    assertEquals(19, c.get(4));
    assertEquals(21, c.get(5));

    ArrayListOfShorts c2 = b.merge(a);
    assertEquals(c, c2);
 }

  @Test
  public void testMerge3() {
    //CASE: one of the lists are empty
    
    ArrayListOfShorts a = new ArrayListOfShorts();
    a.add((short) 3);
    a.add((short) 7);
    a.add((short) 10);
    
    ArrayListOfShorts b = new ArrayListOfShorts();

    ArrayListOfShorts c = a.merge(b);
    assertEquals(c, a);
    
    ArrayListOfShorts c2 = b.merge(a);
    assertEquals(c, c2);   
  }

  @Test
  public void testSubList() {
    ArrayListOfShorts a = new ArrayListOfShorts(new short[] {1, 2, 3, 4, 5, 6, 7});
    ArrayListOfShorts b = a.subList(1, 5);
    assertEquals(5, b.size());
    assertEquals(2, b.get(0));
    assertEquals(3, b.get(1));
    assertEquals(4, b.get(2));
    assertEquals(5, b.get(3));
    assertEquals(6, b.get(4));

    a.clear();
    // Make sure b is a new object.
    assertEquals(5, b.size());
    assertEquals(2, b.get(0));
    assertEquals(3, b.get(1));
    assertEquals(4, b.get(2));
    assertEquals(5, b.get(3));
    assertEquals(6, b.get(4));
  }

  @Test
  public void testAddUnique() {
    ArrayListOfShorts a = new ArrayListOfShorts(new short[] {1, 2, 3, 4, 5, 6, 7});
    a.addUnique(new short[] {8, 0, 2, 5, -1, 11, 9});
    assertEquals(12, a.size());
    assertEquals(0, a.get(8));
    assertEquals(-1, a.get(9));
    assertEquals(11, a.get(10));
    assertEquals(9, a.get(11));
  }

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListOfShortsTest.class);
	}
}