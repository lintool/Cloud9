/*
 *  @(#)HashMap.java	1.73 07/03/13
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package edu.umd.cloud9.util.map;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Hash-based implementation of {@link MapII}.
 */
public class HMapII implements MapII, Cloneable, Serializable {

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
	transient Entry[] table;

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
	public HMapII(int initialCapacity, float loadFactor) {
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
		table = new Entry[capacity];
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
	public HMapII(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	/**
	 * Constructs an empty <tt>HMapII</tt> with the default initial capacity
	 * (1024) and the default load factor (0.75).
	 */
	public HMapII() {
		this.loadFactor = DEFAULT_LOAD_FACTOR;
		threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
		table = new Entry[DEFAULT_INITIAL_CAPACITY];
		init();
	}

	/**
	 * Constructs a new <tt>HMapII</tt> with the same mappings as the
	 * specified <tt>MapII</tt>. The <tt>HMapII</tt> is created with default
	 * load factor (0.75) and an initial capacity sufficient to hold the
	 * mappings in the specified <tt>MapII</tt>.
	 * 
	 * @param m
	 *            the map whose mappings are to be placed in this map
	 * @throws NullPointerException
	 *             if the specified map is null
	 */
	public HMapII(MapII m) {
		this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1, DEFAULT_INITIAL_CAPACITY),
				DEFAULT_LOAD_FACTOR);
		putAllForCreate(m);
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

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public int get(int key) {
		int hash = hash(key);
		for (Entry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
			int k;
			if (e.hash == hash && ((k = e.key) == key || key == k))
				return e.value;
		}

		return DEFAULT_VALUE;
	}

	@Override
	public boolean containsKey(int key) {
		return getEntry(key) != null;
	}

	/**
	 * Returns the entry associated with the specified key in the HMapII.
	 * Returns null if the HMapII contains no mapping for the key.
	 */
	final Entry getEntry(int key) {
		int hash = hash(key);
		for (Entry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
			int k;
			if (e.hash == hash && ((k = e.key) == key || key == k))
				return e;
		}
		return null;
	}

	@Override
	public int put(int key, int value) {
		int hash = hash(key);
		int i = indexFor(hash, table.length);
		for (Entry e = table[i]; e != null; e = e.next) {
			int k;
			if (e.hash == hash && ((k = e.key) == key || key == k)) {
				int oldValue = e.value;
				e.value = value;
				e.recordAccess(this);
				return oldValue;
			}
		}

		modCount++;
		addEntry(hash, key, value, i);
		return DEFAULT_VALUE;
	}

	/**
	 * This method is used instead of put by constructors and pseudoconstructors
	 * (clone, readObject). It does not resize the table, check for
	 * comodification, etc. It calls createEntry rather than addEntry.
	 */
	private void putForCreate(int key, int value) {
		int hash = hash(key);
		int i = indexFor(hash, table.length);

		/**
		 * Look for preexisting entry for key. This will never happen for clone
		 * or deserialize. It will only happen for construction if the input Map
		 * is a sorted map whose ordering is inconsistent w/ equals.
		 */
		for (Entry e = table[i]; e != null; e = e.next) {
			int k;
			if (e.hash == hash && ((k = e.key) == key || key == k)) {
				e.value = value;
				return;
			}
		}

		createEntry(hash, key, value, i);
	}

	private void putAllForCreate(MapII m) {
		for (Iterator<? extends MapII.Entry> i = m.entrySet().iterator(); i.hasNext();) {
			MapII.Entry e = i.next();
			putForCreate(e.getKey(), e.getValue());
		}
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
		Entry[] oldTable = table;
		int oldCapacity = oldTable.length;
		if (oldCapacity == MAXIMUM_CAPACITY) {
			threshold = Integer.MAX_VALUE;
			return;
		}

		Entry[] newTable = new Entry[newCapacity];
		transfer(newTable);
		table = newTable;
		threshold = (int) (newCapacity * loadFactor);
	}

	/**
	 * Transfers all entries from current table to newTable.
	 */
	void transfer(Entry[] newTable) {
		Entry[] src = table;
		int newCapacity = newTable.length;
		for (int j = 0; j < src.length; j++) {
			Entry e = src[j];
			if (e != null) {
				src[j] = null;
				do {
					Entry next = e.next;
					int i = indexFor(e.hash, newCapacity);
					e.next = newTable[i];
					newTable[i] = e;
					e = next;
				} while (e != null);
			}
		}
	}

	@Override
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

	@Override
	public int remove(int key) {
		Entry e = removeEntryForKey(key);
		if (e != null) {
			return e.value;
		}

		throw new NoSuchElementException();
	}

	/**
	 * Removes and returns the entry associated with the specified key in the
	 * HMapII. Returns null if the HMapII contains no mapping for this key.
	 */
	final Entry removeEntryForKey(int key) {
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

		return e;
	}

	/**
	 * Special version of remove for EntrySet.
	 */
	final Entry removeMapping(Object o) {
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

		return e;
	}

	@Override
	public void clear() {
		modCount++;
		Entry[] tab = table;
		for (int i = 0; i < tab.length; i++)
			tab[i] = null;
		size = 0;
	}

	@Override
	public boolean containsValue(int value) {
		Entry[] tab = table;
		for (int i = 0; i < tab.length; i++)
			for (Entry e = tab[i]; e != null; e = e.next)
				if (value == e.value)
					return true;
		return false;
	}

	/**
	 * Returns a shallow copy of this <tt>HMapII</tt> instance: the keys and
	 * values themselves are not cloned.
	 * 
	 * @return a shallow copy of this map
	 */
	@Override
	public Object clone() {
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

		return result;
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

	/**
	 * Adds a new entry with the specified key, value and hash code to the
	 * specified bucket. It is the responsibility of this method to resize the
	 * table if appropriate.
	 * 
	 * Subclass overrides this to alter the behavior of put method.
	 */
	void addEntry(int hash, int key, int value, int bucketIndex) {
		Entry e = table[bucketIndex];
		table[bucketIndex] = new Entry(hash, key, value, e);
		if (size++ >= threshold)
			resize(2 * table.length);
	}

	/**
	 * Like addEntry except that this version is used when creating entries as
	 * part of Map construction or "pseudo-construction" (cloning,
	 * deserialization). This version needn't worry about resizing the table.
	 * 
	 * Subclass overrides this to alter the behavior of HMapII(Map), clone, and
	 * readObject.
	 */
	void createEntry(int hash, int key, int value, int bucketIndex) {
		Entry e = table[bucketIndex];
		table[bucketIndex] = new Entry(hash, key, value, e);
		size++;
	}

	private abstract class HashIterator<E> implements Iterator<E> {
		Entry next; // next entry to return
		int expectedModCount; // For fast-fail
		int index; // current slot
		Entry current; // current entry

		HashIterator() {
			expectedModCount = modCount;
			if (size > 0) { // advance to first entry
				Entry[] t = table;
				while (index < t.length && (next = t[index++]) == null)
					;
			}
		}

		public final boolean hasNext() {
			return next != null;
		}

		final Entry nextEntry() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
			Entry e = next;
			if (e == null)
				throw new NoSuchElementException();

			if ((next = e.next) == null) {
				Entry[] t = table;
				while (index < t.length && (next = t[index++]) == null)
					;
			}
			current = e;
			return e;
		}

		public void remove() {
			if (current == null)
				throw new IllegalStateException();
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
			int k = current.key;
			current = null;
			HMapII.this.removeEntryForKey(k);
			expectedModCount = modCount;
		}

	}

	private final class ValueIterator extends HashIterator<Integer> {
		public Integer next() {
			return nextEntry().value;
		}
	}

	private final class KeyIterator extends HashIterator<Integer> {
		public Integer next() {
			return nextEntry().getKey();
		}
	}

	private final class EntryIterator extends HashIterator<MapII.Entry> {
		public MapII.Entry next() {
			return nextEntry();
		}
	}

	// Subclass overrides these to alter behavior of views' iterator() method
	Iterator<Integer> newKeyIterator() {
		return new KeyIterator();
	}

	Iterator<Integer> newValueIterator() {
		return new ValueIterator();
	}

	Iterator<MapII.Entry> newEntryIterator() {
		return new EntryIterator();
	}

	// Views

	private transient Set<MapII.Entry> entrySet = null;

	/**
	 * Each of these fields are initialized to contain an instance of the
	 * appropriate view the first time this view is requested. The views are
	 * stateless, so there's no reason to create more than one of each.
	 */
	transient volatile Set<Integer> keySet = null;
	transient volatile Collection<Integer> values = null;

	@Override
	public Set<Integer> keySet() {
		Set<Integer> ks = keySet;
		return (ks != null ? ks : (keySet = new KeySet()));
	}

	private final class KeySet extends AbstractSet<Integer> {
		@Override
		public Iterator<Integer> iterator() {
			return newKeyIterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean contains(Object o) {
			return containsKey((Integer) o);
		}
	}

	@Override
	public Collection<Integer> values() {
		Collection<Integer> vs = values;
		return (vs != null ? vs : (values = new Values()));
	}

	private final class Values extends AbstractCollection<Integer> {
		@Override
		public Iterator<Integer> iterator() {
			return newValueIterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean contains(Object o) {
			return containsValue((Integer) o);
		}
	}

	@Override
	public Set<MapII.Entry> entrySet() {
		return entrySet0();
	}

	private Set<MapII.Entry> entrySet0() {
		Set<MapII.Entry> es = entrySet;
		return es != null ? es : (entrySet = new EntrySet());
	}

	private final class EntrySet extends AbstractSet<MapII.Entry> {
		@Override
		public Iterator<MapII.Entry> iterator() {
			return newEntryIterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean contains(Object o) {
			MapII.Entry e = (MapII.Entry) o;
			Entry candidate = getEntry(e.getKey());
			return candidate != null && candidate.equals(e);
		}
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
	private void writeObject(ObjectOutputStream s) throws IOException {
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
		}
	}

	private static final long serialVersionUID = 362498820763181265L;

	/**
	 * Reconstitute the <tt>HMapII</tt> instance from a stream (i.e.,
	 * deserialize it).
	 */
	private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
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
		}
	}

	// These methods are used when serializing HashSets
	int capacity() {
		return table.length;
	}

	float loadFactor() {
		return loadFactor;
	}

	@Override
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
	
	// methods not part of a standard HashMap

	/**
	 * Adds values of keys from another map to this map.
	 * 
	 * @param m
	 *            the other map
	 */
	public void plus(MapII m) {
		for (MapII.Entry e : m.entrySet()) {
			int key = e.getKey();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}
	}

	/**
	 * Computes the dot product of this map with another map.
	 * 
	 * @param m
	 *            the other map
	 */
	public int dot(MapII m) {
		int s = 0;

		for (MapII.Entry e : m.entrySet()) {
			int key = e.getKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getValue();
			}
		}

		return s;
	}

	/**
	 * Increments the key. If the key does not exist in the map, its value is
	 * set to one.
	 * 
	 * @param key
	 *            key to increment
	 */
	public void increment(int key) {
		if (this.containsKey(key)) {
			this.put(key, this.get(key) + 1);
		} else {
			this.put(key, 1);
		}
	}
	
  /**
   * Increments the key by some value. If the key does not exist in the map, its value is
   * set to the parameter value.
   * 
   * @param key
   *            key to increment
   * @param value
   *            increment value
   */
	public void increment(int key, int value) {
		if (this.containsKey(key)) {
			this.put(key, this.get(key) + value);
		} else {
			this.put(key, value);
		}
	}

	/**
	 * Returns entries sorted by descending value. Ties broken by the key.
	 * 
	 * @return entries sorted by descending value
	 */
	public MapII.Entry[] getEntriesSortedByValue() {
		if (this.size() == 0)
			return null;

		// for storing the entries
		MapII.Entry[] entries = new Entry[this.size()];
		int i = 0;
		Entry next = null;

		int index = 0;
		// advance to first entry
		while (index < table.length && (next = table[index++]) == null)
			;

		while (next != null) {
			// current entry
			Entry e = next;

			// advance to next entry
			next = e.next;
			if ((next = e.next) == null) {
				while (index < table.length && (next = table[index++]) == null)
					;
			}

			// add entry to array
			entries[i++] = e;
		}

		// sort the entries
		Arrays.sort(entries, new Comparator<MapII.Entry>() {
			public int compare(MapII.Entry e1, MapII.Entry e2) {
				if (e1.getValue() > e2.getValue()) {
					return -1;
				} else if (e1.getValue() < e2.getValue()) {
					return 1;
				}

				if (e1.getKey() == e2.getKey())
					return 0;

				return e1.getKey() > e2.getKey() ? 1 : -1;
			}
		});

		return entries;
	}

	/**
	 * Returns top <i>n</i> entries sorted by descending value. Ties broken by
	 * the key.
	 * 
	 * @param n
	 *            number of entries to return
	 * @return top <i>n</i> entries sorted by descending value
	 */
	public MapII.Entry[] getEntriesSortedByValue(int n) {
		MapII.Entry[] entries = getEntriesSortedByValue();

		if (entries == null)
			return null;

		if (entries.length < n)
			return entries;

		return Arrays.copyOfRange(entries, 0, n);
	}
}
