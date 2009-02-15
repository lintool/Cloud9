/*
 *  @(#)HashMap.java	1.73 07/03/13
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package edu.umd.cloud9.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Hash-based implementation of the <tt>MapKI</tt> interface. {@link MapKI} is
 * a specialized variant the standard Java {@link Map} interface, except that
 * the keys and values are hard coded as ints for efficiency reasons. This
 * implementation was adapted from {@link HashMap} version 1.73, 03/13/07. See
 * <a href="{@docRoot}/../content/map.html">this benchmark</a> for an
 * efficiency comparison.
 */

public class HMap2II implements MapII, Cloneable, Serializable {

	/**
	 * The default initial capacity - MUST be a power of two.
	 */
	static final int DEFAULT_INITIAL_CAPACITY = 1024;

	/**
	 * The maximum capacity, used if a higher value is implicitly specified by
	 * either of the constructors with arguments. MUST be a power of two <= 1<<30.
	 */
	static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * The load factor used when none specified in constructor.
	 */
	static final float DEFAULT_LOAD_FACTOR = 0.75f;

	/**
	 * The table, resized as necessary. Length MUST Always be a power of two.
	 */
	transient int[] table;
	transient int[] vtable;

	/**
	 * The number of key-value mappings contained in this map.
	 */
	transient int size;

	/**
	 * The next size value at which to resize (capacity * load factor).
	 * 
	 * @serial
	 */
	int threshold;

	/**
	 * The load factor for the hash table.
	 * 
	 * @serial
	 */
	final float loadFactor;

	/**
	 * The number of times this HMapII has been structurally modified Structural
	 * modifications are those that change the number of mappings in the HMapII
	 * or otherwise modify its internal structure (e.g., rehash). This field is
	 * used to make iterators on Collection-views of the HMapII fail-fast. (See
	 * ConcurrentModificationException).
	 */
	transient volatile int modCount;

	/**
	 * Constructs an empty <tt>HMapII</tt> with the specified initial capacity
	 * and load factor.
	 * 
	 * @param initialCapacity
	 *            the initial capacity
	 * @param loadFactor
	 *            the load factor
	 * @throws IllegalArgumentException
	 *             if the initial capacity is negative or the load factor is
	 *             nonpositive
	 */
	public HMap2II(int initialCapacity, float loadFactor) {
		if (initialCapacity < 0)
			throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
		if (initialCapacity > MAXIMUM_CAPACITY)
			initialCapacity = MAXIMUM_CAPACITY;
		if (loadFactor <= 0 || Float.isNaN(loadFactor))
			throw new IllegalArgumentException("Illegal load factor: " + loadFactor);

		// Find a power of 2 >= initialCapacity
		int capacity = 1;
		while (capacity < initialCapacity)
			capacity <<= 1;

		this.loadFactor = loadFactor;
		threshold = (int) (capacity * loadFactor);
		table = new int[capacity];
		vtable = new int[capacity];
		init();
	}

	/**
	 * Constructs an empty <tt>HMapII</tt> with the specified initial capacity
	 * and the default load factor (0.75).
	 * 
	 * @param initialCapacity
	 *            the initial capacity.
	 * @throws IllegalArgumentException
	 *             if the initial capacity is negative.
	 */
	public HMap2II(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	/**
	 * Constructs an empty <tt>HMapII</tt> with the default initial capacity
	 * (1024) and the default load factor (0.75).
	 */
	public HMap2II() {
		this.loadFactor = DEFAULT_LOAD_FACTOR;
		threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
		table = new int[DEFAULT_INITIAL_CAPACITY];
		vtable = new int[DEFAULT_INITIAL_CAPACITY]; 
		init();
	}

	/**
	 * Constructs a new <tt>HMapII</tt> with the same mappings as the
	 * specified <tt>Map</tt>. The <tt>HMapII</tt> is created with default
	 * load factor (0.75) and an initial capacity sufficient to hold the
	 * mappings in the specified <tt>Map</tt>.
	 * 
	 * @param m
	 *            the map whose mappings are to be placed in this map
	 * @throws NullPointerException
	 *             if the specified map is null
	 */
	public HMap2II(MapII m) {
		this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1, DEFAULT_INITIAL_CAPACITY),
				DEFAULT_LOAD_FACTOR);
		throw new UnsupportedOperationException();
		//putAllForCreate(m);
	}

	// internal utilities

	/**
	 * Initialization hook for subclasses. This method is called in all
	 * constructors and pseudo-constructors (clone, readObject) after HMapII has
	 * been initialized but before any entries have been inserted. (In the
	 * absence of this method, readObject would require explicit knowledge of
	 * subclasses.)
	 */
	void init() {
	}

	/**
	 * Applies a supplemental hash function to a given hashCode, which defends
	 * against poor quality hash functions. This is critical because HMapII uses
	 * power-of-two length hash tables, that otherwise encounter collisions for
	 * hashCodes that do not differ in lower bits. Note: Null keys always map to
	 * hash 0, thus index 0.
	 */
	static int hash(int h) {
		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	/**
	 * Returns index for hash code h.
	 */
	static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	// doc copied from interface
	public int size() {
		return size;
	}

	// doc copied from interface
	public boolean isEmpty() {
		return size == 0;
	}

	// doc copied from interface
	public int get(int key) {
		int hash = hash(key);
		//System.out.println("looking up " + key + ", starting at " + indexFor(hash, table.length));
		
		int i;
		for ( i = indexFor(hash, table.length); table[i] != 0;) {
			if ( table[i] == key )
				return vtable[i];
			i++;
			if ( i == table.length) {
				//System.out.println("-wrapping around");
				i=0;
			}

		}

		System.out.println(" couldn't find! " + key + " at " + i);
		
		throw new NoSuchElementException();
	}

	// doc copied from interface
	public boolean containsKey(int key) {
		int hash = hash(key);
		int i;
		for ( i = indexFor(hash, table.length); table[i] != 0;) {
			if ( table[i] == key )
				return true;
			i++;
			if ( i == table.length) {
				//System.out.println("-wrapping around");
				i=0;
			}

		}
		
		return false;
		
	}

	// doc copied from interface
	public void put(int key, int value) {
		int hash = hash(key);
		int i = indexFor(hash, table.length);
		//System.out.println("table length=" + table.length + ", h=" + hash);
		//System.out.println("inserting " + key + ", hash=" + i);
		while ( table[i] != key && table[i] != 0) {
			//System.out.println("Collision!");
			i++;
			if ( i >= table.length ) { //System.out.println("wrapping around!"); 
			i=0; } 
				//resize(2 * table.length);
		}
		//System.out.println(" -insertion point = " + i);
		
		if ( table[i] == key) {
			 vtable[i] = value;
		} else {
		table[i] = key;
		vtable[i] = value;
		if ( size++ > threshold)
			resize(2 * table.length);


		}
		
		
		modCount++;
		
	}

	/**
	 * Rehashes the contents of this map into a new array with a larger
	 * capacity. This method is called automatically when the number of keys in
	 * this map reaches its threshold.
	 * 
	 * If current capacity is MAXIMUM_CAPACITY, this method does not resize the
	 * map, but sets threshold to Integer.MAX_VALUE. This has the effect of
	 * preventing future calls.
	 * 
	 * @param newCapacity
	 *            the new capacity, MUST be a power of two; must be greater than
	 *            current capacity unless current capacity is MAXIMUM_CAPACITY
	 *            (in which case value is irrelevant).
	 */
	void resize(int newCapacity) {
		System.out.println("resizing!");
		
		int[] oldTable = table;
		int oldCapacity = oldTable.length;
		if (oldCapacity == MAXIMUM_CAPACITY) {
			threshold = Integer.MAX_VALUE;
			return;
		}

		int[] newTable = new int[newCapacity];
		int[] newValues = new int[newCapacity];
		transfer(newTable, newValues);
		table = newTable;
		vtable = newValues;
		threshold = (int) (newCapacity * loadFactor);
	}

	/**
	 * Transfers all entries from current table to newTable.
	 */
	void transfer(int[] newTable, int[] newValues) {
		for ( int j=0; j<table.length; j++ ) {

			int key = table[j];
			int hash = hash(key);
			int i = indexFor(hash, newTable.length);
			while ( newTable[i] != key && newTable[i] != 0) {
				i++;
				if ( i >= newTable.length ) i=0; 
			}
			
			newTable[i] = key;
			newValues[i] = vtable[j];			
			
		}
	}

	// doc copied from interface
	public void putAll(MapII m) {
		int numKeysToBeAdded = m.size();
		if (numKeysToBeAdded == 0)
			return;

		/*
		 * Expand the map if the map if the number of mappings to be added is
		 * greater than or equal to threshold. This is conservative; the obvious
		 * condition is (m.size() + size) >= threshold, but this condition could
		 * result in a map with twice the appropriate capacity, if the keys to
		 * be added overlap with the keys already in this map. By using the
		 * conservative calculation, we subject ourself to at most one extra
		 * resize.
		 */
		if (numKeysToBeAdded > threshold) {
			int targetCapacity = (int) (numKeysToBeAdded / loadFactor + 1);
			if (targetCapacity > MAXIMUM_CAPACITY)
				targetCapacity = MAXIMUM_CAPACITY;
			int newCapacity = table.length;
			while (newCapacity < targetCapacity)
				newCapacity <<= 1;
			if (newCapacity > table.length)
				resize(newCapacity);
		}

		for (Iterator<? extends MapII.Entry> i = m.entrySet().iterator(); i.hasNext();) {
			MapII.Entry e = i.next();
			put(e.getKey(), e.getValue());
		}
	}

	// doc copied from interface
	public int remove(int key) {
		Entry e = removeEntryForKey(key);
		if (e != null)
			return e.value;

		throw new NoSuchElementException();
	}

	/**
	 * Removes and returns the entry associated with the specified key in the
	 * HMapII. Returns null if the HMapII contains no mapping for this key.
	 */
	final Entry removeEntryForKey(int key) {
		throw new UnsupportedOperationException();
		/*
		int hash = hash(key);
		int i = indexFor(hash, table.length);
		Entry prev = table[i];
		Entry e = prev;

		while (e != null) {
			Entry next = e.next;
			int k;
			if (e.hash == hash && ((k = e.key) == key || key == k)) {
				modCount++;
				size--;
				if (prev == e)
					table[i] = next;
				else
					prev.next = next;
				e.recordRemoval(this);
				return e;
			}
			prev = e;
			e = next;
		}

		return e;*/
	}

	/**
	 * Special version of remove for EntrySet.
	 */
	final Entry removeMapping(Object o) {
		return null;
		// instanceof is costly, so skip
		// if (!(o instanceof MapII.Entry))
		// return null;

		/*
		MapII.Entry entry = (MapII.Entry) o;
		Object key = entry.getKey();
		int hash = (key == null) ? 0 : hash(key.hashCode());
		int i = indexFor(hash, table.length);
		Entry prev = table[i];
		Entry e = prev;

		while (e != null) {
			Entry next = e.next;
			if (e.hash == hash && e.equals(entry)) {
				modCount++;
				size--;
				if (prev == e)
					table[i] = next;
				else
					prev.next = next;
				e.recordRemoval(this);
				return e;
			}
			prev = e;
			e = next;
		}

		return e;*/
	}

	// doc copied from interface
	public void clear() {
		/*
		modCount++;
		Entry[] tab = table;
		for (int i = 0; i < tab.length; i++)
			tab[i] = null;
		size = 0;*/
	}

	// doc copied from interface
	public boolean containsValue(int value) {
		/*
		Entry[] tab = table;
		for (int i = 0; i < tab.length; i++)
			for (Entry e = tab[i]; e != null; e = e.next)
				if (value == e.value)
					return true;*/
		return false;
	}

	/**
	 * Returns a shallow copy of this <tt>HMapII</tt> instance: the keys and
	 * values themselves are not cloned.
	 * 
	 * @return a shallow copy of this map
	 */
	public Object clone() {
		/*
		HMapII result = null;
		try {
			result = (HMapII) super.clone();
		} catch (CloneNotSupportedException e) {
			// assert false;
		}
		result.table = new Entry[table.length];
		result.entrySet = null;
		result.modCount = 0;
		result.size = 0;
		result.init();
		result.putAllForCreate(this);

		return result;*/
		return null;
	}

	static class Entry implements MapII.Entry {
		final int key;
		int value;
		Entry next;
		final int hash;

		/**
		 * Creates new entry.
		 */
		Entry(int h, int k, int v, Entry n) {
			value = v;
			next = n;
			key = k;
			hash = h;
		}

		public final int getKey() {
			return key;
		}

		public final int getValue() {
			return value;
		}

		public final int setValue(int newValue) {
			int oldValue = value;
			value = newValue;
			return oldValue;
		}

		public final boolean equals(Object o) {
			// if (!(o instanceof MapKI.Entry))
			// return false;
			MapII.Entry e = (MapII.Entry) o;
			int k1 = getKey();
			int k2 = e.getKey();
			if (k1 == k2) {
				int v1 = getValue();
				int v2 = e.getValue();
				if (v1 == v2)
					return true;
			}
			return false;
		}

		public final int hashCode() {
			return (key) ^ (value);
		}

		public final String toString() {
			return getKey() + "=" + getValue();
		}

		/**
		 * This method is invoked whenever the value in an entry is overwritten
		 * by an invocation of put(k,v) for a key k that's already in the
		 * HMapII.
		 */
		void recordAccess(HMapII m) {
		}

		/**
		 * This method is invoked whenever the entry is removed from the table.
		 */
		void recordRemoval(HMapII m) {
		}
	}


	// doc copied from interface
	public Set<Integer> keySet() {
		return null;
	}

	// doc copied from interface
	public Collection<Integer> values() {
		return null;
	}

	// doc copied from interface
	public Set<MapII.Entry> entrySet() {
		return null;
	}
	
	/**
	 * Save the state of the <tt>HMapII</tt> instance to a stream (i.e.,
	 * serialize it).
	 * 
	 * @serialData The <i>capacity</i> of the HMapII (the length of the bucket
	 *             array) is emitted (int), followed by the <i>size</i> (an
	 *             int, the number of key-value mappings), followed by the key
	 *             (Object) and value (Object) for each key-value mapping. The
	 *             key-value mappings are emitted in no particular order.
	 */
	private void writeObject(java.io.ObjectOutputStream s) throws IOException {
		/*
		Iterator<MapII.Entry> i = (size > 0) ? entrySet0().iterator() : null;

		// Write out the threshold, loadfactor, and any hidden stuff
		s.defaultWriteObject();

		// Write out number of buckets
		s.writeInt(table.length);

		// Write out size (number of Mappings)
		s.writeInt(size);

		// Write out keys and values (alternating)
		if (i != null) {
			while (i.hasNext()) {
				MapII.Entry e = i.next();
				s.writeInt(e.getKey());
				s.writeInt(e.getValue());
			}
		}*/
	}

	private static final long serialVersionUID = 362498820763181265L;

	/**
	 * Reconstitute the <tt>HMapII</tt> instance from a stream (i.e.,
	 * deserialize it).
	 */
	private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
		/*
		// Read in the threshold, loadfactor, and any hidden stuff
		s.defaultReadObject();

		// Read in number of buckets and allocate the bucket array;
		int numBuckets = s.readInt();
		table = new Entry[numBuckets];

		init(); // Give subclass a chance to do its thing.

		// Read in size (number of Mappings)
		int size = s.readInt();

		// Read the keys and values, and put the mappings in the HMapII
		for (int i = 0; i < size; i++) {
			int key = s.readInt();
			int value = s.readInt();
			putForCreate(key, value);
		}*/
	}

	// These methods are used when serializing HashSets
	int capacity() {
		return table.length;
	}

	float loadFactor() {
		return loadFactor;
	}

	public String toString() {
		Iterator<MapII.Entry> i = entrySet().iterator();
		if (!i.hasNext())
			return "{}";

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
			MapII.Entry e = i.next();
			int key = e.getKey();
			int value = e.getValue();
			sb.append(key);
			sb.append('=');
			sb.append(value);
			if (!i.hasNext())
				return sb.append('}').toString();
			sb.append(", ");
		}
	}
}
