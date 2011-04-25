/*
 *  @(#)HashMap.java	1.73 07/03/13
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package edu.umd.cloud9.util.map;

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
 * Hash-based implementation of the <tt>MapKL</tt> interface. {@link MapKL} is
 * a specialized variant the standard Java {@link Map} interface, except that
 * the values are hard coded as floats for efficiency reasons (keys can be
 * arbitrary objects). This implementation was adapted from {@link HashMap}
 * version 1.73, 03/13/07. See <a href="{@docRoot}/../content/map.html">this
 * benchmark</a> for an efficiency comparison.
 * 
 * @param <K>
 *            the type of keys maintained by this map
 */

public class HMapKL<K> implements MapKL<K>, Cloneable, Serializable {

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
	transient Entry<K>[] table;

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
	 * The number of times this HMapKL has been structurally modified Structural
	 * modifications are those that change the number of mappings in the HMapKL
	 * or otherwise modify its internal structure (e.g., rehash). This field is
	 * used to make iterators on Collection-views of the HMapKL fail-fast. (See
	 * ConcurrentModificationException).
	 */
	transient volatile int modCount;

	/**
	 * Constructs an empty <tt>HMapKL</tt> with the specified initial capacity
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
	@SuppressWarnings("unchecked")
	public HMapKL(int initialCapacity, float loadFactor) {
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
	 * Constructs an empty <tt>HMapKL</tt> with the specified initial capacity
	 * and the default load factor (0.75).
	 * 
	 * @param initialCapacity
	 *            the initial capacity.
	 * @throws IllegalArgumentException
	 *             if the initial capacity is negative.
	 */
	public HMapKL(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	/**
	 * Constructs an empty <tt>HMapKL</tt> with the default initial capacity
	 * (1024) and the default load factor (0.75).
	 */
	@SuppressWarnings("unchecked")
	public HMapKL() {
		this.loadFactor = DEFAULT_LOAD_FACTOR;
		threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
		table = new Entry[DEFAULT_INITIAL_CAPACITY];
		init();
	}

	/**
	 * Constructs a new <tt>HMapKL</tt> with the same mappings as the
	 * specified <tt>MapKL</tt>. The <tt>HMapKL</tt> is created with default
	 * load factor (0.75) and an initial capacity sufficient to hold the
	 * mappings in the specified <tt>MapKL</tt>.
	 * 
	 * @param m
	 *            the map whose mappings are to be placed in this map
	 * @throws NullPointerException
	 *             if the specified map is null
	 */
	public HMapKL(MapKL<? extends K> m) {
		this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1, DEFAULT_INITIAL_CAPACITY),
				DEFAULT_LOAD_FACTOR);
		putAllForCreate(m);
	}

	// internal utilities

	/**
	 * Initialization hook for subclasses. This method is called in all
	 * constructors and pseudo-constructors (clone, readObject) after HMapKL has
	 * been initialized but before any entries have been inserted. (In the
	 * absence of this method, readObject would require explicit knowledge of
	 * subclasses.)
	 */
	void init() {
	}

	/**
	 * Applies a supplemental hash function to a given hashCode, which defends
	 * against poor quality hash functions. This is critical because HMapKL uses
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
	public long get(K key) {
		if (key == null)
			return getForNullKey();
		int hash = hash(key.hashCode());
		for (Entry<K> e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
			Object k;
			if (e.hash == hash && ((k = e.key) == key || key.equals(k)))
				return e.value;
		}

		return DEFAULT_VALUE;
	}

	/**
	 * Offloaded version of get() to look up null keys. Null keys map to index
	 * 0. This null case is split out into separate methods for the sake of
	 * performance in the two most commonly used operations (get and put), but
	 * incorporated with conditionals in others.
	 */
	private long getForNullKey() {
		for (Entry<K> e = table[0]; e != null; e = e.next) {
			if (e.key == null)
				return e.value;
		}

		return DEFAULT_VALUE;
	}

	// doc copied from interface
	public boolean containsKey(K key) {
		return getEntry(key) != null;
	}

	/**
	 * Returns the entry associated with the specified key in the HMapKL.
	 * Returns null if the HMapKL contains no mapping for the key.
	 */
	final Entry<K> getEntry(Object key) {
		int hash = (key == null) ? 0 : hash(key.hashCode());
		for (Entry<K> e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
			Object k;
			if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k))))
				return e;
		}
		return null;
	}

	 /**
   * Increments the key. If the key does not exist in the map, its value is
   * set to one.
   * 
   * @param key
   *            key to increment
   */
  public void increment(K key) {
    if (this.containsKey(key)) {
      this.put(key, (long) this.get(key) + 1);
    } else {
      this.put(key, (long) 1);
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
  public void increment(K key, long value) {
    if (this.containsKey(key)) {
      this.put(key, (long) this.get(key) + value);
    } else {
      this.put(key, value);
    }
  }

	// doc copied from interface
	public long put(K key, long value) {
		if (key == null) {
			return putForNullKey(value);
		}
		int hash = hash(key.hashCode());
		int i = indexFor(hash, table.length);
		for (Entry<K> e = table[i]; e != null; e = e.next) {
			Object k;
			if (e.hash == hash && ((k = e.key) == key || key.equals(k))) {
			  long oldValue = e.value;
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
	 * Offloaded version of put for null keys
	 */
	private long putForNullKey(long value) {
		for (Entry<K> e = table[0]; e != null; e = e.next) {
			if (e.key == null) {
			  long oldValue = e.value;
				e.value = value;
				e.recordAccess(this);
				return oldValue;
			}
		}
		
		modCount++;
		addEntry(0, null, value, 0);
		return DEFAULT_VALUE;
	}

	/**
	 * This method is used instead of put by constructors and pseudoconstructors
	 * (clone, readObject). It does not resize the table, check for
	 * comodification, etc. It calls createEntry rather than addEntry.
	 */
	private void putForCreate(K key, long value) {
		int hash = (key == null) ? 0 : hash(key.hashCode());
		int i = indexFor(hash, table.length);

		/**
		 * Look for preexisting entry for key. This will never happen for clone
		 * or deserialize. It will only happen for construction if the input Map
		 * is a sorted map whose ordering is inconsistent w/ equals.
		 */
		for (Entry<K> e = table[i]; e != null; e = e.next) {
			Object k;
			if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
				e.value = value;
				return;
			}
		}

		createEntry(hash, key, value, i);
	}

	private void putAllForCreate(MapKL<? extends K> m) {
		for (Iterator<? extends MapKL.Entry<? extends K>> i = m.entrySet().iterator(); i.hasNext();) {
			MapKL.Entry<? extends K> e = i.next();
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
	@SuppressWarnings("unchecked")
	void resize(int newCapacity) {
		Entry<K>[] oldTable = table;
		int oldCapacity = oldTable.length;
		if (oldCapacity == MAXIMUM_CAPACITY) {
			threshold = Integer.MAX_VALUE;
			return;
		}

		Entry<K>[] newTable = new Entry[newCapacity];
		transfer(newTable);
		table = newTable;
		threshold = (int) (newCapacity * loadFactor);
	}

	/**
	 * Transfers all entries from current table to newTable.
	 */
	void transfer(Entry<K>[] newTable) {
		Entry<K>[] src = table;
		int newCapacity = newTable.length;
		for (int j = 0; j < src.length; j++) {
			Entry<K> e = src[j];
			if (e != null) {
				src[j] = null;
				do {
					Entry<K> next = e.next;
					int i = indexFor(e.hash, newCapacity);
					e.next = newTable[i];
					newTable[i] = e;
					e = next;
				} while (e != null);
			}
		}
	}

	// doc copied from interface
	public void putAll(MapKL<? extends K> m) {
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

		for (Iterator<? extends MapKL.Entry<? extends K>> i = m.entrySet().iterator(); i.hasNext();) {
			MapKL.Entry<? extends K> e = i.next();
			put(e.getKey(), e.getValue());
		}
	}

	// doc copied from interface
	public long remove(K key) {
		Entry<K> e = removeEntryForKey(key);
		if (e != null)
			return e.value;

		throw new NoSuchElementException();
	}

	/**
	 * Removes and returns the entry associated with the specified key in the
	 * HMapKL. Returns null if the HMapKL contains no mapping for this key.
	 */
	final Entry<K> removeEntryForKey(Object key) {
		int hash = (key == null) ? 0 : hash(key.hashCode());
		int i = indexFor(hash, table.length);
		Entry<K> prev = table[i];
		Entry<K> e = prev;

		while (e != null) {
			Entry<K> next = e.next;
			Object k;
			if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
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
	@SuppressWarnings("unchecked")
	final Entry<K> removeMapping(Object o) {
		if (!(o instanceof Map.Entry))
			return null;

		MapKL.Entry<K> entry = (MapKL.Entry<K>) o;
		Object key = entry.getKey();
		int hash = (key == null) ? 0 : hash(key.hashCode());
		int i = indexFor(hash, table.length);
		Entry<K> prev = table[i];
		Entry<K> e = prev;

		while (e != null) {
			Entry<K> next = e.next;
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

	// doc copied from interface
	public void clear() {
		modCount++;
		Entry<K>[] tab = table;
		for (int i = 0; i < tab.length; i++)
			tab[i] = null;
		size = 0;
	}

	// doc copied from interface
	public boolean containsValue(long value) {
		Entry<K>[] tab = table;
		for (int i = 0; i < tab.length; i++)
			for (Entry<K> e = tab[i]; e != null; e = e.next)
				if (value == e.value)
					return true;
		return false;
	}

	/**
	 * Returns a shallow copy of this <tt>HMapKL</tt> instance: the keys and
	 * values themselves are not cloned.
	 * 
	 * @return a shallow copy of this map
	 */
	@SuppressWarnings("unchecked")
	public Object clone() {
		HMapKL<K> result = null;
		try {
			result = (HMapKL<K>) super.clone();
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

	static class Entry<K> implements MapKL.Entry<K> {
		final K key;
		long value;
		Entry<K> next;
		final int hash;

		/**
		 * Creates new entry.
		 */
		Entry(int h, K k, long v, Entry<K> n) {
			value = v;
			next = n;
			key = k;
			hash = h;
		}

		public final K getKey() {
			return key;
		}

		public final long getValue() {
			return value;
		}

		public final long setValue(long newValue) {
			long oldValue = value;
			value = newValue;
			return oldValue;
		}

		@SuppressWarnings("unchecked")
		public final boolean equals(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			MapKL.Entry<K> e = (MapKL.Entry<K>) o;
			K k1 = getKey();
			K k2 = e.getKey();
			if (k1 == k2 || (k1 != null && k1.equals(k2))) {
				long v1 = getValue();
				long v2 = e.getValue();
				if (v1 == v2)
					return true;
			}
			return false;
		}

		public final int hashCode() {
			return (key == null ? 0 : key.hashCode()) ^ ((int) value);
		}

		public final String toString() {
			return getKey() + "=" + getValue();
		}

		/**
		 * This method is invoked whenever the value in an entry is overwritten
		 * by an invocation of put(k,v) for a key k that's already in the
		 * HMapKL.
		 */
		void recordAccess(HMapKL<K> m) {
		}

		/**
		 * This method is invoked whenever the entry is removed from the table.
		 */
		void recordRemoval(HMapKL<K> m) {
		}
	}

	/**
	 * Adds a new entry with the specified key, value and hash code to the
	 * specified bucket. It is the responsibility of this method to resize the
	 * table if appropriate.
	 * 
	 * Subclass overrides this to alter the behavior of put method.
	 */
	void addEntry(int hash, K key, long value, int bucketIndex) {
		Entry<K> e = table[bucketIndex];
		table[bucketIndex] = new Entry<K>(hash, key, value, e);
		if (size++ >= threshold)
			resize(2 * table.length);
	}

	/**
	 * Like addEntry except that this version is used when creating entries as
	 * part of Map construction or "pseudo-construction" (cloning,
	 * deserialization). This version needn't worry about resizing the table.
	 * 
	 * Subclass overrides this to alter the behavior of HMapKL(Map), clone, and
	 * readObject.
	 */
	void createEntry(int hash, K key, long value, int bucketIndex) {
		Entry<K> e = table[bucketIndex];
		table[bucketIndex] = new Entry<K>(hash, key, value, e);
		size++;
	}

	private abstract class HashIterator<E> implements Iterator<E> {
		Entry<K> next; // next entry to return
		int expectedModCount; // For fast-fail
		int index; // current slot
		Entry<K> current; // current entry

		HashIterator() {
			expectedModCount = modCount;
			if (size > 0) { // advance to first entry
				Entry<K>[] t = table;
				while (index < t.length && (next = t[index++]) == null)
					;
			}
		}

		public final boolean hasNext() {
			return next != null;
		}

		final Entry<K> nextEntry() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
			Entry<K> e = next;
			if (e == null)
				throw new NoSuchElementException();

			if ((next = e.next) == null) {
				Entry<K>[] t = table;
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
			Object k = current.key;
			current = null;
			HMapKL.this.removeEntryForKey(k);
			expectedModCount = modCount;
		}

	}

	private final class ValueIterator extends HashIterator<Long> {
		public Long next() {
			return nextEntry().value;
		}
	}

	private final class KeyIterator extends HashIterator<K> {
		public K next() {
			return nextEntry().getKey();
		}
	}

	private final class EntryIterator extends HashIterator<MapKL.Entry<K>> {
		public MapKL.Entry<K> next() {
			return nextEntry();
		}
	}

	// Subclass overrides these to alter behavior of views' iterator() method
	Iterator<K> newKeyIterator() {
		return new KeyIterator();
	}

	Iterator<Long> newValueIterator() {
		return new ValueIterator();
	}

	Iterator<MapKL.Entry<K>> newEntryIterator() {
		return new EntryIterator();
	}

	// Views

	private transient Set<MapKL.Entry<K>> entrySet = null;

	/**
	 * Each of these fields are initialized to contain an instance of the
	 * appropriate view the first time this view is requested. The views are
	 * stateless, so there's no reason to create more than one of each.
	 */
	transient volatile Set<K> keySet = null;
	transient volatile Collection<Long> values = null;

	// doc copied from interface
	public Set<K> keySet() {
		Set<K> ks = keySet;
		return (ks != null ? ks : (keySet = new KeySet()));
	}

	private final class KeySet extends AbstractSet<K> {
		@Override
		public Iterator<K> iterator() {
			return newKeyIterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override @SuppressWarnings("unchecked")
		public boolean contains(Object o) {
			return containsKey((K) o);
		}
	}

	// doc copied from interface
	public Collection<Long> values() {
		Collection<Long> vs = values;
		return (vs != null ? vs : (values = new Values()));
	}

	private final class Values extends AbstractCollection<Long> {
		@Override
		public Iterator<Long> iterator() {
			return newValueIterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean contains(Object o) {
			return containsValue((Long) o);
		}
	}

	// doc copied from interface
	public Set<MapKL.Entry<K>> entrySet() {
		return entrySet0();
	}

	private Set<MapKL.Entry<K>> entrySet0() {
		Set<MapKL.Entry<K>> es = entrySet;
		return es != null ? es : (entrySet = new EntrySet());
	}

	private final class EntrySet extends AbstractSet<MapKL.Entry<K>> {
		@Override
		public Iterator<MapKL.Entry<K>> iterator() {
			return newEntryIterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override @SuppressWarnings("unchecked")
		public boolean contains(Object o) {
			MapKL.Entry<K> e = (MapKL.Entry<K>) o;
			Entry<K> candidate = getEntry(e.getKey());
			return candidate != null && candidate.equals(e);
		}
	}

	/**
	 * Save the state of the <tt>HMapKL</tt> instance to a stream (i.e.,
	 * serialize it).
	 * 
	 * @serialData The <i>capacity</i> of the HMapKL (the length of the bucket
	 *             array) is emitted (int), followed by the <i>size</i> (an
	 *             int, the number of key-value mappings), followed by the key
	 *             (Object) and value (Object) for each key-value mapping. The
	 *             key-value mappings are emitted in no particular order.
	 */
	private void writeObject(java.io.ObjectOutputStream s) throws IOException {
		Iterator<MapKL.Entry<K>> i = (size > 0) ? entrySet0().iterator() : null;

		// Write out the threshold, loadfactor, and any hidden stuff
		s.defaultWriteObject();

		// Write out number of buckets
		s.writeInt(table.length);

		// Write out size (number of Mappings)
		s.writeInt(size);

		// Write out keys and values (alternating)
		if (i != null) {
			while (i.hasNext()) {
				MapKL.Entry<K> e = i.next();
				s.writeObject(e.getKey());
				s.writeLong(e.getValue());
			}
		}
	}

	private static final long serialVersionUID = 362498820763181265L;

	/**
	 * Reconstitute the <tt>HMapKL</tt> instance from a stream (i.e.,
	 * deserialize it).
	 */
	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
		// Read in the threshold, loadfactor, and any hidden stuff
		s.defaultReadObject();

		// Read in number of buckets and allocate the bucket array;
		int numBuckets = s.readInt();
		table = new Entry[numBuckets];

		init(); // Give subclass a chance to do its thing.

		// Read in size (number of Mappings)
		int size = s.readInt();

		// Read the keys and values, and put the mappings in the HMapKL
		for (int i = 0; i < size; i++) {
			K key = (K) s.readObject();
			long value = s.readLong();
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

	public String toString() {
		Iterator<MapKL.Entry<K>> i = entrySet().iterator();
		if (!i.hasNext())
			return "{}";

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
			MapKL.Entry<K> e = i.next();
			K key = e.getKey();
			long value = e.getValue();
			sb.append(key);
			sb.append('=');
			sb.append(value);
			if (!i.hasNext())
				return sb.append('}').toString();
			sb.append(", ");
		}
	}

}
