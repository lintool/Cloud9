// jdk-6u23-fcs-src-b05-jrl-12_nov_2010.jar

package edu.umd.cloud9.util.map;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.umd.cloud9.util.map.MapIV.Entry;

/**
 * A Red-Black tree based {@link NavigableMap} implementation. The map is sorted according to the
 * {@linkplain Comparable natural ordering} of its keys, or by a {@link Comparator} provided at map
 * creation time, depending on which constructor is used.
 * 
 * <p>
 * This implementation provides guaranteed log(n) time cost for the <tt>containsKey</tt>,
 * <tt>get</tt>, <tt>put</tt> and <tt>remove</tt> operations. Algorithms are adaptations of those in
 * Cormen, Leiserson, and Rivest's <I>Introduction to Algorithms</I>.
 * 
 * <p>
 * Note that the ordering maintained by a sorted map (whether or not an explicit comparator is
 * provided) must be <i>consistent with equals</i> if this sorted map is to correctly implement the
 * <tt>Map</tt> interface. (See <tt>Comparable</tt> or <tt>Comparator</tt> for a precise definition
 * of <i>consistent with equals</i>.) This is so because the <tt>Map</tt> interface is defined in
 * terms of the equals operation, but a map performs all key comparisons using its
 * <tt>compareTo</tt> (or <tt>compare</tt>) method, so two keys that are deemed equal by this method
 * are, from the standpoint of the sorted map, equal. The behavior of a sorted map <i>is</i>
 * well-defined even if its ordering is inconsistent with equals; it just fails to obey the general
 * contract of the <tt>Map</tt> interface.
 * 
 * <p>
 * <strong>Note that this implementation is not synchronized.</strong> If multiple threads access a
 * map concurrently, and at least one of the threads modifies the map structurally, it <i>must</i>
 * be synchronized externally. (A structural modification is any operation that adds or deletes one
 * or more mappings; merely changing the value associated with an existing key is not a structural
 * modification.) This is typically accomplished by synchronizing on some object that naturally
 * encapsulates the map. If no such object exists, the map should be "wrapped" using the
 * {@link Collections#synchronizedSortedMap Collections.synchronizedSortedMap} method. This is best
 * done at creation time, to prevent accidental unsynchronized access to the map:
 * 
 * <pre>
 *   SortedMap m = Collections.synchronizedSortedMap(new TMapIV(...));
 * </pre>
 * 
 * <p>
 * The iterators returned by the <tt>iterator</tt> method of the collections returned by all of this
 * class's "collection view methods" are <i>fail-fast</i>: if the map is structurally modified at
 * any time after the iterator is created, in any way except through the iterator's own
 * <tt>remove</tt> method, the iterator will throw a {@link ConcurrentModificationException}. Thus,
 * in the face of concurrent modification, the iterator fails quickly and cleanly, rather than
 * risking arbitrary, non-deterministic behavior at an undetermined time in the future.
 * 
 * <p>
 * Note that the fail-fast behavior of an iterator cannot be guaranteed as it is, generally
 * speaking, impossible to make any hard guarantees in the presence of unsynchronized concurrent
 * modification. Fail-fast iterators throw <tt>ConcurrentModificationException</tt> on a best-effort
 * basis. Therefore, it would be wrong to write a program that depended on this exception for its
 * correctness: <i>the fail-fast behavior of iterators should be used only to detect bugs.</i>
 * 
 * <p>
 * All <tt>MapIV.Entry</tt> pairs returned by methods in this class and its views represent
 * snapshots of mappings at the time they were produced. They do <em>not</em> support the
 * <tt>Entry.setValue</tt> method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>.)
 * 
 * <p>
 * This class is a member of the <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 * 
 * @param <K>
 *          the type of keys maintained by this map
 * @param <V>
 *          the type of mapped values
 * 
 * @author Josh Bloch and Doug Lea
 * @version 1.73, 05/10/06
 * @see Map
 * @see HashMap
 * @see Hashtable
 * @see Comparable
 * @see Comparator
 * @see Collection
 * @since 1.2
 */

public class TMapIV<V> implements NavigableMapIV<V>, Cloneable, java.io.Serializable {
  private transient Entry<V> root = null;

  /**
   * The number of entries in the tree
   */
  private transient int size = 0;

  /**
   * The number of structural modifications to the tree.
   */
  private transient int modCount = 0;

  /**
   * Constructs a new, empty tree map, using the natural ordering of its keys. All keys inserted
   * into the map must implement the {@link Comparable} interface. Furthermore, all such keys must
   * be <i>mutually comparable</i>: <tt>k1.compareTo(k2)</tt> must not throw a
   * <tt>ClassCastException</tt> for any keys <tt>k1</tt> and <tt>k2</tt> in the map. If the user
   * attempts to put a key into the map that violates this constraint (for example, the user
   * attempts to put a string key into a map whose keys are integers), the
   * <tt>put(Object key, Object value)</tt> call will throw a <tt>ClassCastException</tt>.
   */
  public TMapIV() {
  }

  /**
   * Constructs a new tree map containing the same mappings as the given map, ordered according to
   * the <i>natural ordering</i> of its keys. All keys inserted into the new map must implement the
   * {@link Comparable} interface. Furthermore, all such keys must be <i>mutually comparable</i>:
   * <tt>k1.compareTo(k2)</tt> must not throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt>
   * and <tt>k2</tt> in the map. This method runs in n*log(n) time.
   * 
   * @param m
   *          the map whose mappings are to be placed in this map
   * @throws ClassCastException
   *           if the keys in m are not {@link Comparable}, or are not mutually comparable
   * @throws NullPointerException
   *           if the specified map is null
   */
  public TMapIV(MapIV<V> m) {
    putAll(m);
  }

  /**
   * Constructs a new tree map containing the same mappings and using the same ordering as the
   * specified sorted map. This method runs in linear time.
   * 
   * @param m
   *          the sorted map whose mappings are to be placed in this map, and whose comparator is to
   *          be used to sort this map
   * @throws NullPointerException
   *           if the specified map is null
   */
  public TMapIV(SortedMapIV<? extends V> m) {
    try {
      buildFromSorted(m.size(), m.entrySet().iterator(), null, null);
    } catch (java.io.IOException cannotHappen) {
    } catch (ClassNotFoundException cannotHappen) {
    }
  }

  // Query Operations

  /**
   * Returns the number of key-value mappings in this map.
   * 
   * @return the number of key-value mappings in this map
   */
  public int size() {
    return size;
  }

  /**
   * Returns <tt>true</tt> if this map contains a mapping for the specified key.
   * 
   * @param key
   *          key whose presence in this map is to be tested
   * @return <tt>true</tt> if this map contains a mapping for the specified key
   * @throws ClassCastException
   *           if the specified key cannot be compared with the keys currently in the map
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   */
  public boolean containsKey(int key) {
    return getEntry(key) != null;
  }

  /**
   * Returns <tt>true</tt> if this map maps one or more keys to the specified value. More formally,
   * returns <tt>true</tt> if and only if this map contains at least one mapping to a value
   * <tt>v</tt> such that <tt>(value==null ? v==null : value.equals(v))</tt>. This operation will
   * probably require time linear in the map size for most implementations.
   * 
   * @param value
   *          value whose presence in this map is to be tested
   * @return <tt>true</tt> if a mapping to <tt>value</tt> exists; <tt>false</tt> otherwise
   * @since 1.2
   */
  public boolean containsValue(Object value) {
    for (Entry<V> e = getFirstEntry(); e != null; e = successor(e))
      if (valEquals(value, e.value))
        return true;
    return false;
  }

  /**
   * Returns the value to which the specified key is mapped, or {@code null} if this map contains no
   * mapping for the key.
   * 
   * <p>
   * More formally, if this map contains a mapping from a key {@code k} to a value {@code v} such
   * that {@code key} compares equal to {@code k} according to the map's ordering, then this method
   * returns {@code v}; otherwise it returns {@code null}. (There can be at most one such mapping.)
   * 
   * <p>
   * A return value of {@code null} does not <i>necessarily</i> indicate that the map contains no
   * mapping for the key; it's also possible that the map explicitly maps the key to {@code null}.
   * The {@link #containsKey containsKey} operation may be used to distinguish these two cases.
   * 
   * @throws ClassCastException
   *           if the specified key cannot be compared with the keys currently in the map
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   */
  public V get(int key) {
    Entry<V> p = getEntry(key);
    return (p == null ? null : p.value);
  }

  /**
   * @throws NoSuchElementException
   *           {@inheritDoc}
   */
  public int firstKey() {
    return getFirstEntry().key;
  }

  /**
   * @throws NoSuchElementException
   *           {@inheritDoc}
   */
  public int lastKey() {
    return getLastEntry().key;
  }

  /**
   * Copies all of the mappings from the specified map to this map. These mappings replace any
   * mappings that this map had for any of the keys currently in the specified map.
   * 
   * @param map
   *          mappings to be stored in this map
   * @throws ClassCastException
   *           if the class of a key or value in the specified map prevents it from being stored in
   *           this map
   * @throws NullPointerException
   *           if the specified map is null or the specified map contains a null key and this map
   *           does not permit null keys
   */
  public void putAll(MapIV<V> m) {
    for (MapIV.Entry<V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Returns this map's entry for the given key, or <tt>null</tt> if the map does not contain an
   * entry for the key.
   * 
   * @return this map's entry for the given key, or <tt>null</tt> if the map does not contain an
   *         entry for the key
   * @throws ClassCastException
   *           if the specified key cannot be compared with the keys currently in the map
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   */
  final Entry<V> getEntry(int key) {
    Entry<V> p = root;
    while (p != null) {
      int cmp = compare(key, p.key);
      if (cmp < 0)
        p = p.left;
      else if (cmp > 0)
        p = p.right;
      else
        return p;
    }
    return null;
  }

  /**
   * Gets the entry corresponding to the specified key; if no such entry exists, returns the entry
   * for the least key greater than the specified key; if no such entry exists (i.e., the greatest
   * key in the Tree is less than the specified key), returns <tt>null</tt>.
   */
  final Entry<V> getCeilingEntry(int key) {
    Entry<V> p = root;
    while (p != null) {
      int cmp = compare(key, p.key);
      if (cmp < 0) {
        if (p.left != null)
          p = p.left;
        else
          return p;
      } else if (cmp > 0) {
        if (p.right != null) {
          p = p.right;
        } else {
          Entry<V> parent = p.parent;
          Entry<V> ch = p;
          while (parent != null && ch == parent.right) {
            ch = parent;
            parent = parent.parent;
          }
          return parent;
        }
      } else
        return p;
    }
    return null;
  }

  /**
   * Gets the entry corresponding to the specified key; if no such entry exists, returns the entry
   * for the greatest key less than the specified key; if no such entry exists, returns
   * <tt>null</tt>.
   */
  final Entry<V> getFloorEntry(int key) {
    Entry<V> p = root;
    while (p != null) {
      int cmp = compare(key, p.key);
      if (cmp > 0) {
        if (p.right != null)
          p = p.right;
        else
          return p;
      } else if (cmp < 0) {
        if (p.left != null) {
          p = p.left;
        } else {
          Entry<V> parent = p.parent;
          Entry<V> ch = p;
          while (parent != null && ch == parent.left) {
            ch = parent;
            parent = parent.parent;
          }
          return parent;
        }
      } else
        return p;

    }
    return null;
  }

  /**
   * Gets the entry for the least key greater than the specified key; if no such entry exists,
   * returns the entry for the least key greater than the specified key; if no such entry exists
   * returns <tt>null</tt>.
   */
  final Entry<V> getHigherEntry(int key) {
    Entry<V> p = root;
    while (p != null) {
      int cmp = compare(key, p.key);
      if (cmp < 0) {
        if (p.left != null)
          p = p.left;
        else
          return p;
      } else {
        if (p.right != null) {
          p = p.right;
        } else {
          Entry<V> parent = p.parent;
          Entry<V> ch = p;
          while (parent != null && ch == parent.right) {
            ch = parent;
            parent = parent.parent;
          }
          return parent;
        }
      }
    }
    return null;
  }

  /**
   * Returns the entry for the greatest key less than the specified key; if no such entry exists
   * (i.e., the least key in the Tree is greater than the specified key), returns <tt>null</tt>.
   */
  final Entry<V> getLowerEntry(int key) {
    Entry<V> p = root;
    while (p != null) {
      int cmp = compare(key, p.key);
      if (cmp > 0) {
        if (p.right != null)
          p = p.right;
        else
          return p;
      } else {
        if (p.left != null) {
          p = p.left;
        } else {
          Entry<V> parent = p.parent;
          Entry<V> ch = p;
          while (parent != null && ch == parent.left) {
            ch = parent;
            parent = parent.parent;
          }
          return parent;
        }
      }
    }
    return null;
  }

  /**
   * Associates the specified value with the specified key in this map. If the map previously
   * contained a mapping for the key, the old value is replaced.
   * 
   * @param key
   *          key with which the specified value is to be associated
   * @param value
   *          value to be associated with the specified key
   * 
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no
   *         mapping for <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map
   *         previously associated <tt>null</tt> with <tt>key</tt>.)
   * @throws ClassCastException
   *           if the specified key cannot be compared with the keys currently in the map
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   */
  public V put(int key, V value) {
    Entry<V> t = root;
    if (t == null) {
      root = new Entry<V>(key, value, null);
      size = 1;
      modCount++;
      return null;
    }
    int cmp;
    Entry<V> parent;
    do {
      parent = t;
      cmp = compare(key, t.key);
      if (cmp < 0)
        t = t.left;
      else if (cmp > 0)
        t = t.right;
      else
        return t.setValue(value);
    } while (t != null);
    Entry<V> e = new Entry<V>(key, value, parent);
    if (cmp < 0)
      parent.left = e;
    else
      parent.right = e;
    fixAfterInsertion(e);
    size++;
    modCount++;
    return null;
  }

  /**
   * Removes the mapping for this key from this TMapIV if present.
   * 
   * @param key
   *          key for which mapping should be removed
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no
   *         mapping for <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map
   *         previously associated <tt>null</tt> with <tt>key</tt>.)
   * @throws ClassCastException
   *           if the specified key cannot be compared with the keys currently in the map
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   */
  public V remove(int key) {
    Entry<V> p = getEntry(key);
    if (p == null)
      return null;

    V oldValue = p.value;
    deleteEntry(p);
    return oldValue;
  }

  /**
   * Removes all of the mappings from this map. The map will be empty after this call returns.
   */
  public void clear() {
    modCount++;
    size = 0;
    root = null;
  }

  /**
   * Returns a shallow copy of this <tt>TMapIV</tt> instance. (The keys and values themselves are
   * not cloned.)
   * 
   * @return a shallow copy of this map
   */
  @SuppressWarnings("unchecked")
  public Object clone() {
    TMapIV<V> clone = null;
    try {
      clone = (TMapIV<V>) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new InternalError();
    }

    // Put clone into "virgin" state (except for comparator)
    clone.root = null;
    clone.size = 0;
    clone.modCount = 0;
    clone.entrySet = null;
    clone.navigableKeySet = null;
    clone.descendingMap = null;

    // Initialize clone with our mappings
    try {
      clone.buildFromSorted(size, entrySet().iterator(), null, null);
    } catch (java.io.IOException cannotHappen) {
    } catch (ClassNotFoundException cannotHappen) {
    }

    return clone;
  }

  // NavigableMap API methods

  /**
   * @since 1.6
   */
  public MapIV.Entry<V> firstEntry() {
    return exportEntry(getFirstEntry());
  }

  /**
   * @since 1.6
   */
  public MapIV.Entry<V> lastEntry() {
    return exportEntry(getLastEntry());
  }

  /**
   * @since 1.6
   */
  public MapIV.Entry<V> pollFirstEntry() {
    Entry<V> p = getFirstEntry();
    MapIV.Entry<V> result = exportEntry(p);
    if (p != null)
      deleteEntry(p);
    return result;
  }

  /**
   * @since 1.6
   */
  public MapIV.Entry<V> pollLastEntry() {
    Entry<V> p = getLastEntry();
    MapIV.Entry<V> result = exportEntry(p);
    if (p != null)
      deleteEntry(p);
    return result;
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public MapIV.Entry<V> lowerEntry(int key) {
    return exportEntry(getLowerEntry(key));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public int lowerKey(int key) {
    return getLowerEntry(key).key;
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public MapIV.Entry<V> floorEntry(int key) {
    return exportEntry(getFloorEntry(key));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public int floorKey(int key) {
    return getFloorEntry(key).key;
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public MapIV.Entry<V> ceilingEntry(int key) {
    return exportEntry(getCeilingEntry(key));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public int ceilingKey(int key) {
    return getCeilingEntry(key).key;
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public MapIV.Entry<V> higherEntry(int key) {
    return exportEntry(getHigherEntry(key));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified key is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @since 1.6
   */
  public int higherKey(int key) {
    return getHigherEntry(key).key;
  }

  // Views

  /**
   * Fields initialized to contain an instance of the entry set view the first time this view is
   * requested. Views are stateless, so there's no reason to create more than one.
   */
  private transient EntrySet entrySet = null;
  private transient KeySet navigableKeySet = null;
  private transient NavigableMapIV<V> descendingMap = null;

  /**
   * Returns a {@link Set} view of the keys contained in this map. The set's iterator returns the
   * keys in ascending order. The set is backed by the map, so changes to the map are reflected in
   * the set, and vice-versa. If the map is modified while an iteration over the set is in progress
   * (except through the iterator's own <tt>remove</tt> operation), the results of the iteration are
   * undefined. The set supports element removal, which removes the corresponding mapping from the
   * map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>,
   * <tt>retainAll</tt>, and <tt>clear</tt> operations. It does not support the <tt>add</tt> or
   * <tt>addAll</tt> operations.
   */
  public Set<Integer> keySet() {
    return navigableKeySet();
  }

  /**
   * @since 1.6
   */
  public NavigableSet<Integer> navigableKeySet() {
    KeySet nks = navigableKeySet;
    return (nks != null) ? nks : (navigableKeySet = new KeySet(this));
  }

  /**
   * @since 1.6
   */
  public NavigableSet<Integer> descendingKeySet() {
    return descendingMap().navigableKeySet();
  }

  /**
   * Returns a {@link Collection} view of the values contained in this map. The collection's
   * iterator returns the values in ascending order of the corresponding keys. The collection is
   * backed by the map, so changes to the map are reflected in the collection, and vice-versa. If
   * the map is modified while an iteration over the collection is in progress (except through the
   * iterator's own <tt>remove</tt> operation), the results of the iteration are undefined. The
   * collection supports element removal, which removes the corresponding mapping from the map, via
   * the <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>, <tt>removeAll</tt>,
   * <tt>retainAll</tt> and <tt>clear</tt> operations. It does not support the <tt>add</tt> or
   * <tt>addAll</tt> operations.
   */
  public Collection<V> values() {
    return (vs != null) ? vs : (vs = new Values());
  }

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set's iterator returns
   * the entries in ascending key order. The set is backed by the map, so changes to the map are
   * reflected in the set, and vice-versa. If the map is modified while an iteration over the set is
   * in progress (except through the iterator's own <tt>remove</tt> operation, or through the
   * <tt>setValue</tt> operation on a map entry returned by the iterator) the results of the
   * iteration are undefined. The set supports element removal, which removes the corresponding
   * mapping from the map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>
   * , <tt>retainAll</tt> and <tt>clear</tt> operations. It does not support the <tt>add</tt> or
   * <tt>addAll</tt> operations.
   */
  public Set<MapIV.Entry<V>> entrySet() {
    EntrySet es = entrySet;
    return (es != null) ? es : (entrySet = new EntrySet());
  }

  /**
   * @since 1.6
   */
  public NavigableMapIV<V> descendingMap() {
    NavigableMapIV<V> km = descendingMap;
    return (km != null) ? km : (descendingMap = new DescendingSubMap<V>(this, true, Integer.MIN_VALUE,
        true, true, Integer.MAX_VALUE, true));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if <tt>fromKey</tt> or <tt>toKey</tt> is null and this map uses natural ordering, or
   *           its comparator does not permit null keys
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   * @since 1.6
   */
  public NavigableMapIV<V> subMap(int fromKey, boolean fromInclusive, int toKey, boolean toInclusive) {
    return new AscendingSubMap<V>(this, false, fromKey, fromInclusive, false, toKey, toInclusive);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if <tt>toKey</tt> is null and this map uses natural ordering, or its comparator does
   *           not permit null keys
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   * @since 1.6
   */
  public NavigableMapIV<V> headMap(int toKey, boolean inclusive) {
    return new AscendingSubMap<V>(this, true, Integer.MIN_VALUE, true, false, toKey, inclusive);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if <tt>fromKey</tt> is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   * @since 1.6
   */
  public NavigableMapIV<V> tailMap(int fromKey, boolean inclusive) {
    return new AscendingSubMap<V>(this, false, fromKey, inclusive, true, Integer.MAX_VALUE, true);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if <tt>fromKey</tt> or <tt>toKey</tt> is null and this map uses natural ordering, or
   *           its comparator does not permit null keys
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   */
  public SortedMapIV<V> subMap(int fromKey, int toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if <tt>toKey</tt> is null and this map uses natural ordering, or its comparator does
   *           not permit null keys
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   */
  public SortedMapIV<V> headMap(int toKey) {
    return headMap(toKey, false);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if <tt>fromKey</tt> is null and this map uses natural ordering, or its comparator
   *           does not permit null keys
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   */
  public SortedMapIV<V> tailMap(int fromKey) {
    return tailMap(fromKey, true);
  }

  // View class support

  Collection<V> vs;

  class Values extends AbstractCollection<V> {
    public Iterator<V> iterator() {
      return new ValueIterator(getFirstEntry());
    }

    public int size() {
      return TMapIV.this.size();
    }

    public boolean contains(Object o) {
      return TMapIV.this.containsValue(o);
    }

    public boolean remove(Object o) {
      for (Entry<V> e = getFirstEntry(); e != null; e = successor(e)) {
        if (valEquals(e.getValue(), o)) {
          deleteEntry(e);
          return true;
        }
      }
      return false;
    }

    public void clear() {
      TMapIV.this.clear();
    }
  }

  class EntrySet extends AbstractSet<MapIV.Entry<V>> {
    public Iterator<MapIV.Entry<V>> iterator() {
      return new EntryIterator(getFirstEntry());
    }

    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
      if (!(o instanceof MapIV.Entry))
        return false;
      MapIV.Entry<V> entry = (MapIV.Entry<V>) o;
      V value = entry.getValue();
      Entry<V> p = getEntry(entry.getKey());
      return p != null && valEquals(p.getValue(), value);
    }

    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
      if (!(o instanceof MapIV.Entry))
        return false;
      MapIV.Entry<V> entry = (MapIV.Entry<V>) o;
      V value = entry.getValue();
      Entry<V> p = getEntry(entry.getKey());
      if (p != null && valEquals(p.getValue(), value)) {
        deleteEntry(p);
        return true;
      }
      return false;
    }

    public int size() {
      return TMapIV.this.size();
    }

    public void clear() {
      TMapIV.this.clear();
    }
  }

  /*
   * Unlike Values and EntrySet, the KeySet class is static, delegating to a NavigableMap to allow
   * use by SubMaps, which outweighs the ugliness of needing type-tests for the following Iterator
   * methods that are defined appropriately in main versus submap classes.
   */

  Iterator<Integer> keyIterator() {
    return new KeyIterator(getFirstEntry());
  }

  Iterator<Integer> descendingKeyIterator() {
    return new DescendingKeyIterator(getLastEntry());
  }

  static final class KeySet extends AbstractSet<Integer> implements NavigableSet<Integer> {
    private final NavigableMapIV<?> m;

    KeySet(NavigableMapIV<?> map) {
      m = map;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Integer> iterator() {
      if (m instanceof TMapIV) {
        return ((TMapIV<Object>) m).keyIterator();
      } else {
        return (Iterator<Integer>) (((TMapIV.NavigableSubMap<?>) m).keyIterator());
      }
    }

    @SuppressWarnings("unchecked")
    public Iterator<Integer> descendingIterator() {
      if (m instanceof TreeMap)
        return ((TMapIV<Object>) m).descendingKeyIterator();
      else
        return (Iterator<Integer>) (((TMapIV.NavigableSubMap) m).descendingKeyIterator());
    }

    public int size() {
      return m.size();
    }

    public boolean isEmpty() {
      return m.isEmpty();
    }

    public boolean contains(int o) {
      return m.containsKey(o);
    }

    public void clear() {
      m.clear();
    }

    public Integer lower(Integer e) {
      return m.lowerKey(e);
    }

    public Integer floor(Integer e) {
      return m.floorKey(e);
    }

    public Integer ceiling(Integer e) {
      return m.ceilingKey(e);
    }

    public Integer higher(Integer e) {
      return m.higherKey(e);
    }

    public Integer first() {
      return m.firstKey();
    }

    public Integer last() {
      return m.lastKey();
    }

    public Integer pollFirst() {
      MapIV.Entry<?> e = m.pollFirstEntry();
      return e == null ? null : e.getKey();
    }

    public Integer pollLast() {
      MapIV.Entry<?> e = m.pollLastEntry();
      return e == null ? null : e.getKey();
    }

    public boolean remove(int o) {
      int oldSize = size();
      m.remove(o);
      return size() != oldSize;
    }

    public NavigableSet<Integer> subSet(int fromElement, boolean fromInclusive, int toElement,
        boolean toInclusive) {
      NavigableMapIV<?> nmap = m.subMap(fromElement, fromInclusive, toElement, toInclusive);
      TreeSet<Integer> t = new TreeSet<Integer>();
      for (MapIV.Entry<?> entry : nmap.entrySet()) {
        t.add(entry.getKey());
      }
      return t;
    }

    public NavigableSet<Integer> headSet(int toElement, boolean inclusive) {
      NavigableMapIV<?> nmap = m.headMap(toElement, inclusive);
      TreeSet<Integer> t = new TreeSet<Integer>();
      for (MapIV.Entry<?> entry : nmap.entrySet()) {
        t.add(entry.getKey());
      }
      return t;
    }

    public NavigableSet<Integer> tailSet(int fromElement, boolean inclusive) {
      NavigableMapIV<?> nmap = m.tailMap(fromElement, inclusive);
      TreeSet<Integer> t = new TreeSet<Integer>();
      for (MapIV.Entry<?> entry : nmap.entrySet()) {
        t.add(entry.getKey());
      }
      return t;
    }

    public SortedSet<Integer> subSet(int fromElement, int toElement) {
      return subSet(fromElement, true, toElement, false);
    }

    public SortedSet<Integer> headSet(int toElement) {
      return headSet(toElement, false);
    }

    public SortedSet<Integer> tailSet(int fromElement) {
      return tailSet(fromElement, true);
    }

    public NavigableSet<Integer> descendingSet() {
      NavigableMapIV<?> nmap = m.descendingMap();
      TreeSet<Integer> t = new TreeSet<Integer>();
      for (MapIV.Entry<?> entry : nmap.entrySet()) {
        t.add(entry.getKey());
      }
      return t;
    }

    @Override
    public SortedSet<Integer> headSet(Integer toElement) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public NavigableSet<Integer> headSet(Integer toElement, boolean inclusive) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public SortedSet<Integer> subSet(Integer fromElement, Integer toElement) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public NavigableSet<Integer> subSet(Integer fromElement, boolean fromInclusive,
        Integer toElement, boolean toInclusive) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public SortedSet<Integer> tailSet(Integer fromElement) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public NavigableSet<Integer> tailSet(Integer fromElement, boolean inclusive) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Comparator<? super Integer> comparator() {
      // TODO Auto-generated method stub
      return null;
    }
  }

  /**
   * Base class for TMapIV Iterators
   */
  abstract class PrivateEntryIterator<T> implements Iterator<T> {
    Entry<V> next;
    Entry<V> lastReturned;
    int expectedModCount;

    PrivateEntryIterator(Entry<V> first) {
      expectedModCount = modCount;
      lastReturned = null;
      next = first;
    }

    public final boolean hasNext() {
      return next != null;
    }

    final Entry<V> nextEntry() {
      Entry<V> e = next;
      if (e == null)
        throw new NoSuchElementException();
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      next = successor(e);
      lastReturned = e;
      return e;
    }

    final Entry<V> prevEntry() {
      Entry<V> e = next;
      if (e == null)
        throw new NoSuchElementException();
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      next = predecessor(e);
      lastReturned = e;
      return e;
    }

    public void remove() {
      if (lastReturned == null)
        throw new IllegalStateException();
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      // deleted entries are replaced by their successors
      if (lastReturned.left != null && lastReturned.right != null)
        next = lastReturned;
      deleteEntry(lastReturned);
      expectedModCount = modCount;
      lastReturned = null;
    }
  }

  final class EntryIterator extends PrivateEntryIterator<MapIV.Entry<V>> {
    EntryIterator(Entry<V> first) {
      super(first);
    }

    public MapIV.Entry<V> next() {
      return nextEntry();
    }
  }

  final class ValueIterator extends PrivateEntryIterator<V> {
    ValueIterator(Entry<V> first) {
      super(first);
    }

    public V next() {
      return nextEntry().value;
    }
  }

  final class KeyIterator extends PrivateEntryIterator<Integer> {
    KeyIterator(Entry<V> first) {
      super(first);
    }

    public Integer next() {
      return nextEntry().key;
    }
  }

  final class DescendingKeyIterator extends PrivateEntryIterator<Integer> {
    DescendingKeyIterator(Entry<V> first) {
      super(first);
    }

    public Integer next() {
      return prevEntry().key;
    }
  }

  // Little utilities

  /**
   * Compares two keys using the correct comparison method for this TMapIV.
   */
  final int compare(int k1, int k2) {
    return k1 == k2 ? 0 : k1 > k2 ? 1 : -1;
  }

  /**
   * Test two values for equality. Differs from o1.equals(o2) only in that it copes with
   * <tt>null</tt> o1 properly.
   */
  final static boolean valEquals(Object o1, Object o2) {
    return (o1 == null ? o2 == null : o1.equals(o2));
  }

  /**
   * Return SimpleImmutableEntry for entry, or null if null
   */
  static <K, V> MapIV.Entry<V> exportEntry(TMapIV.Entry<V> e) {
    return e == null ? null : new SimpleImmutableEntry<V>(e);
  }

  public static class SimpleImmutableEntry<V> implements MapIV.Entry<V>, java.io.Serializable {
    private static final long serialVersionUID = 7138329143949025153L;

    private final int key;
    private final V value;

    /**
     * Creates an entry representing a mapping from the specified key to the specified value.
     * 
     * @param key
     *          the key represented by this entry
     * @param value
     *          the value represented by this entry
     */
    public SimpleImmutableEntry(int key, V value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Creates an entry representing the same mapping as the specified entry.
     * 
     * @param entry
     *          the entry to copy
     */
    public SimpleImmutableEntry(Entry<? extends V> entry) {
      this.key = entry.getKey();
      this.value = entry.getValue();
    }

    /**
     * Returns the key corresponding to this entry.
     * 
     * @return the key corresponding to this entry
     */
    public int getKey() {
      return key;
    }

    /**
     * Returns the value corresponding to this entry.
     * 
     * @return the value corresponding to this entry
     */
    public V getValue() {
      return value;
    }

    /**
     * Replaces the value corresponding to this entry with the specified value (optional operation).
     * This implementation simply throws <tt>UnsupportedOperationException</tt>, as this class
     * implements an <i>immutable</i> map entry.
     * 
     * @param value
     *          new value to be stored in this entry
     * @return (Does not return)
     * @throws UnsupportedOperationException
     *           always
     */
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }

    /**
     * Compares the specified object with this entry for equality. Returns {@code true} if the given
     * object is also a map entry and the two entries represent the same mapping. More formally, two
     * entries {@code e1} and {@code e2} represent the same mapping if
     * 
     * <pre>
     * (e1.getKey() == null ? e2.getKey() == null : e1.getKey().equals(e2.getKey()))
     *     &amp;&amp; (e1.getValue() == null ? e2.getValue() == null : e1.getValue().equals(e2.getValue()))
     * </pre>
     * 
     * This ensures that the {@code equals} method works properly across different implementations
     * of the {@code Map.Entry} interface.
     * 
     * @param o
     *          object to be compared for equality with this map entry
     * @return {@code true} if the specified object is equal to this map entry
     * @see #hashCode
     */
    public boolean equals(Object o) {
      if (!(o instanceof MapIV.Entry))
        return false;
      MapIV.Entry e = (MapIV.Entry) o;
      return key == e.getKey() && valEquals(value, e.getValue());
    }

    /**
     * Returns the hash code value for this map entry. The hash code of a map entry {@code e} is
     * defined to be:
     * 
     * <pre>
     * (e.getKey() == null ? 0 : e.getKey().hashCode())
     *     &circ; (e.getValue() == null ? 0 : e.getValue().hashCode())
     * </pre>
     * 
     * This ensures that {@code e1.equals(e2)} implies that {@code e1.hashCode()==e2.hashCode()} for
     * any two Entries {@code e1} and {@code e2}, as required by the general contract of
     * {@link Object#hashCode}.
     * 
     * @return the hash code value for this map entry
     * @see #equals
     */
    public int hashCode() {
      return key ^ (value == null ? 0 : value.hashCode());
    }

    /**
     * Returns a String representation of this map entry. This implementation returns the string
     * representation of this entry's key followed by the equals character ("<tt>=</tt>") followed
     * by the string representation of this entry's value.
     * 
     * @return a String representation of this map entry
     */
    public String toString() {
      return key + "=" + value;
    }

  }

  // SubMaps

  /**
   * @serial include
   */
  static abstract class NavigableSubMap<V> implements NavigableMapIV<V>, java.io.Serializable {
    /**
     * The backing map.
     */
    final TMapIV<V> m;

    /**
     * Endpoints are represented as triples (fromStart, lo, loInclusive) and (toEnd, hi,
     * hiInclusive). If fromStart is true, then the low (absolute) bound is the start of the backing
     * map, and the other values are ignored. Otherwise, if loInclusive is true, lo is the inclusive
     * bound, else lo is the exclusive bound. Similarly for the upper bound.
     */
    final int lo, hi;
    final boolean fromStart, toEnd;
    final boolean loInclusive, hiInclusive;

    NavigableSubMap(TMapIV<V> m, boolean fromStart, int lo, boolean loInclusive, boolean toEnd,
        int hi, boolean hiInclusive) {
      if (!fromStart && !toEnd) {
        if (m.compare(lo, hi) > 0)
          throw new IllegalArgumentException("fromKey > toKey");
      } else {
        if (!fromStart) // type check
          m.compare(lo, lo);
        if (!toEnd)
          m.compare(hi, hi);
      }

      this.m = m;
      this.fromStart = fromStart;
      this.lo = lo;
      this.loInclusive = loInclusive;
      this.toEnd = toEnd;
      this.hi = hi;
      this.hiInclusive = hiInclusive;
    }

    // internal utilities

    final boolean tooLow(int key) {
      if (!fromStart) {
        int c = m.compare(key, lo);
        if (c < 0 || (c == 0 && !loInclusive))
          return true;
      }
      return false;
    }

    final boolean tooHigh(int key) {
      if (!toEnd) {
        int c = m.compare(key, hi);
        if (c > 0 || (c == 0 && !hiInclusive))
          return true;
      }
      return false;
    }

    final boolean inRange(int key) {
      return !tooLow(key) && !tooHigh(key);
    }

    final boolean inClosedRange(int key) {
      return (fromStart || m.compare(key, lo) >= 0) && (toEnd || m.compare(hi, key) >= 0);
    }

    final boolean inRange(int key, boolean inclusive) {
      return inclusive ? inRange(key) : inClosedRange(key);
    }

    /*
     * Absolute versions of relation operations. Subclasses map to these using like-named "sub"
     * versions that invert senses for descending maps
     */

    final TMapIV.Entry<V> absLowest() {
      TMapIV.Entry<V> e = (fromStart ? m.getFirstEntry() : (loInclusive ? m.getCeilingEntry(lo) : m
          .getHigherEntry(lo)));
      return (e == null || tooHigh(e.key)) ? null : e;
    }

    final TMapIV.Entry<V> absHighest() {
      TMapIV.Entry<V> e = (toEnd ? m.getLastEntry() : (hiInclusive ? m.getFloorEntry(hi) : m
          .getLowerEntry(hi)));
      return (e == null || tooLow(e.key)) ? null : e;
    }

    final TMapIV.Entry<V> absCeiling(int key) {
      if (tooLow(key))
        return absLowest();
      TMapIV.Entry<V> e = m.getCeilingEntry(key);
      return (e == null || tooHigh(e.key)) ? null : e;
    }

    final TMapIV.Entry<V> absHigher(int key) {
      if (tooLow(key))
        return absLowest();
      TMapIV.Entry<V> e = m.getHigherEntry(key);
      return (e == null || tooHigh(e.key)) ? null : e;
    }

    final TMapIV.Entry<V> absFloor(int key) {
      if (tooHigh(key))
        return absHighest();
      TMapIV.Entry<V> e = m.getFloorEntry(key);
      return (e == null || tooLow(e.key)) ? null : e;
    }

    final TMapIV.Entry<V> absLower(int key) {
      if (tooHigh(key))
        return absHighest();
      TMapIV.Entry<V> e = m.getLowerEntry(key);
      return (e == null || tooLow(e.key)) ? null : e;
    }

    /** Returns the absolute high fence for ascending traversal */
    final TMapIV.Entry<V> absHighFence() {
      return (toEnd ? null : (hiInclusive ? m.getHigherEntry(hi) : m.getCeilingEntry(hi)));
    }

    /** Return the absolute low fence for descending traversal */
    final TMapIV.Entry<V> absLowFence() {
      return (fromStart ? null : (loInclusive ? m.getLowerEntry(lo) : m.getFloorEntry(lo)));
    }

    // Abstract methods defined in ascending vs descending classes
    // These relay to the appropriate absolute versions

    abstract TMapIV.Entry<V> subLowest();

    abstract TMapIV.Entry<V> subHighest();

    abstract TMapIV.Entry<V> subCeiling(int key);

    abstract TMapIV.Entry<V> subHigher(int key);

    abstract TMapIV.Entry<V> subFloor(int key);

    abstract TMapIV.Entry<V> subLower(int key);

    /** Returns ascending iterator from the perspective of this submap */
    abstract Iterator<Integer> keyIterator();

    /** Returns descending iterator from the perspective of this submap */
    abstract Iterator<Integer> descendingKeyIterator();

    // public methods

    public boolean isEmpty() {
      return (fromStart && toEnd) ? m.isEmpty() : entrySet().isEmpty();
    }

    public int size() {
      return (fromStart && toEnd) ? m.size() : entrySet().size();
    }

    public final boolean containsKey(int key) {
      return inRange(key) && m.containsKey(key);
    }

    public final V put(int key, V value) {
      if (!inRange(key))
        throw new IllegalArgumentException("key out of range");
      return m.put(key, value);
    }

    public final V get(int key) {
      return !inRange(key) ? null : m.get(key);
    }

    public final V remove(int key) {
      return !inRange(key) ? null : m.remove(key);
    }

    public final MapIV.Entry<V> ceilingEntry(int key) {
      return exportEntry(subCeiling(key));
    }

    public final int ceilingKey(int key) {
      return subCeiling(key).key;
    }

    public final MapIV.Entry<V> higherEntry(int key) {
      return exportEntry(subHigher(key));
    }

    public final int higherKey(int key) {
      return subHigher(key).key;
    }

    public final MapIV.Entry<V> floorEntry(int key) {
      return exportEntry(subFloor(key));
    }

    public final int floorKey(int key) {
      return subFloor(key).key;
    }

    public final MapIV.Entry<V> lowerEntry(int key) {
      return exportEntry(subLower(key));
    }

    public final int lowerKey(int key) {
      return subLower(key).key;
    }

    public final int firstKey() {
      return subLowest().key;
    }

    public final int lastKey() {
      return subHighest().key;
    }

    public final MapIV.Entry<V> firstEntry() {
      return exportEntry(subLowest());
    }

    public final MapIV.Entry<V> lastEntry() {
      return exportEntry(subHighest());
    }

    public final MapIV.Entry<V> pollFirstEntry() {
      TMapIV.Entry<V> e = subLowest();
      MapIV.Entry<V> result = exportEntry(e);
      if (e != null)
        m.deleteEntry(e);
      return result;
    }

    public final MapIV.Entry<V> pollLastEntry() {
      TMapIV.Entry<V> e = subHighest();
      MapIV.Entry<V> result = exportEntry(e);
      if (e != null)
        m.deleteEntry(e);
      return result;
    }

    // Views
    transient NavigableMapIV<V> descendingMapView = null;
    transient EntrySetView entrySetView = null;
    transient KeySet navigableKeySetView = null;

    public final NavigableSet<Integer> navigableKeySet() {
      KeySet nksv = navigableKeySetView;
      return (nksv != null) ? nksv : (navigableKeySetView = new TMapIV.KeySet(this));
    }

    public final Set<Integer> keySet() {
      return navigableKeySet();
    }

    public NavigableSet<Integer> descendingKeySet() {
      return descendingMap().navigableKeySet();
    }

    public final SortedMapIV<V> subMap(int fromKey, int toKey) {
      return subMap(fromKey, true, toKey, false);
    }

    public final SortedMapIV<V> headMap(int toKey) {
      return headMap(toKey, false);
    }

    public final SortedMapIV<V> tailMap(int fromKey) {
      return tailMap(fromKey, true);
    }

    // View classes

    abstract class EntrySetView extends AbstractSet<MapIV.Entry<V>> {
      private transient int size = -1, sizeModCount;

      public int size() {
        if (fromStart && toEnd)
          return m.size();
        if (size == -1 || sizeModCount != m.modCount) {
          sizeModCount = m.modCount;
          size = 0;
          Iterator<Entry<V>> i = iterator();
          while (i.hasNext()) {
            size++;
            i.next();
          }
        }
        return size;
      }

      public boolean isEmpty() {
        TMapIV.Entry<V> n = absLowest();
        return n == null || tooHigh(n.key);
      }

      @SuppressWarnings("unchecked")
      public boolean contains(Object o) {
        if (!(o instanceof MapIV.Entry))
          return false;
        MapIV.Entry<V> entry = (MapIV.Entry<V>) o;
        int key = entry.getKey();
        if (!inRange(key))
          return false;
        TMapIV.Entry<V> node = m.getEntry(key);
        return node != null && valEquals(node.getValue(), entry.getValue());
      }

      @SuppressWarnings("unchecked")
      public boolean remove(Object o) {
        if (!(o instanceof MapIV.Entry))
          return false;
        MapIV.Entry<V> entry = (MapIV.Entry<V>) o;
        int key = entry.getKey();
        if (!inRange(key))
          return false;
        TMapIV.Entry<V> node = m.getEntry(key);
        if (node != null && valEquals(node.getValue(), entry.getValue())) {
          m.deleteEntry(node);
          return true;
        }
        return false;
      }
    }

    /**
     * Iterators for SubMaps
     */
    abstract class SubMapIterator<T> implements Iterator<T> {
      TMapIV.Entry<V> lastReturned;
      TMapIV.Entry<V> next;
      final int fenceKey;
      int expectedModCount;

      SubMapIterator(TMapIV.Entry<V> first, TMapIV.Entry<V> fence) {
        expectedModCount = m.modCount;
        lastReturned = null;
        next = first;
        fenceKey = fence == null ? null : fence.key;
      }

      public final boolean hasNext() {
        return next != null && next.key != fenceKey;
      }

      final TMapIV.Entry<V> nextEntry() {
        TMapIV.Entry<V> e = next;
        if (e == null || e.key == fenceKey)
          throw new NoSuchElementException();
        if (m.modCount != expectedModCount)
          throw new ConcurrentModificationException();
        next = successor(e);
        lastReturned = e;
        return e;
      }

      final TMapIV.Entry<V> prevEntry() {
        TMapIV.Entry<V> e = next;
        if (e == null || e.key == fenceKey)
          throw new NoSuchElementException();
        if (m.modCount != expectedModCount)
          throw new ConcurrentModificationException();
        next = predecessor(e);
        lastReturned = e;
        return e;
      }

      final void removeAscending() {
        if (lastReturned == null)
          throw new IllegalStateException();
        if (m.modCount != expectedModCount)
          throw new ConcurrentModificationException();
        // deleted entries are replaced by their successors
        if (lastReturned.left != null && lastReturned.right != null)
          next = lastReturned;
        m.deleteEntry(lastReturned);
        lastReturned = null;
        expectedModCount = m.modCount;
      }

      final void removeDescending() {
        if (lastReturned == null)
          throw new IllegalStateException();
        if (m.modCount != expectedModCount)
          throw new ConcurrentModificationException();
        m.deleteEntry(lastReturned);
        lastReturned = null;
        expectedModCount = m.modCount;
      }

    }

    final class SubMapEntryIterator extends SubMapIterator<MapIV.Entry<V>> {
      SubMapEntryIterator(TMapIV.Entry<V> first, TMapIV.Entry<V> fence) {
        super(first, fence);
      }

      public MapIV.Entry<V> next() {
        return nextEntry();
      }

      public void remove() {
        removeAscending();
      }
    }

    final class SubMapKeyIterator extends SubMapIterator<Integer> {
      SubMapKeyIterator(TMapIV.Entry<V> first, TMapIV.Entry<V> fence) {
        super(first, fence);
      }

      public Integer next() {
        return nextEntry().key;
      }

      public void remove() {
        removeAscending();
      }
    }

    final class DescendingSubMapEntryIterator extends SubMapIterator<MapIV.Entry<V>> {
      DescendingSubMapEntryIterator(TMapIV.Entry<V> last, TMapIV.Entry<V> fence) {
        super(last, fence);
      }

      public MapIV.Entry<V> next() {
        return prevEntry();
      }

      public void remove() {
        removeDescending();
      }
    }

    final class DescendingSubMapKeyIterator extends SubMapIterator<Integer> {
      DescendingSubMapKeyIterator(TMapIV.Entry<V> last, TMapIV.Entry<V> fence) {
        super(last, fence);
      }

      public Integer next() {
        return prevEntry().key;
      }

      public void remove() {
        removeDescending();
      }
    }
  }

  /**
   * @serial include
   */
  static final class AscendingSubMap<V> extends NavigableSubMap<V> {
    private static final long serialVersionUID = 912986545866124060L;

    AscendingSubMap(TMapIV<V> m, boolean fromStart, int lo, boolean loInclusive, boolean toEnd,
        int hi, boolean hiInclusive) {
      super(m, fromStart, lo, loInclusive, toEnd, hi, hiInclusive);
    }

    public NavigableMapIV<V> subMap(int fromKey, boolean fromInclusive, int toKey,
        boolean toInclusive) {
      if (!inRange(fromKey, fromInclusive))
        throw new IllegalArgumentException("fromKey out of range");
      if (!inRange(toKey, toInclusive))
        throw new IllegalArgumentException("toKey out of range");
      return new AscendingSubMap<V>(m, false, fromKey, fromInclusive, false, toKey, toInclusive);
    }

    public NavigableMapIV<V> headMap(int toKey, boolean inclusive) {
      if (!inRange(toKey, inclusive))
        throw new IllegalArgumentException("toKey out of range");
      return new AscendingSubMap<V>(m, fromStart, lo, loInclusive, false, toKey, inclusive);
    }

    public NavigableMapIV<V> tailMap(int fromKey, boolean inclusive) {
      if (!inRange(fromKey, inclusive))
        throw new IllegalArgumentException("fromKey out of range");
      return new AscendingSubMap<V>(m, false, fromKey, inclusive, toEnd, hi, hiInclusive);
    }

    public NavigableMapIV<V> descendingMap() {
      NavigableMapIV<V> mv = descendingMapView;
      return (mv != null) ? mv : (descendingMapView = new DescendingSubMap<V>(m, fromStart, lo,
          loInclusive, toEnd, hi, hiInclusive));
    }

    Iterator<Integer> keyIterator() {
      return new SubMapKeyIterator(absLowest(), absHighFence());
    }

    Iterator<Integer> descendingKeyIterator() {
      return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
    }

    final class AscendingEntrySetView extends EntrySetView {
      public Iterator<MapIV.Entry<V>> iterator() {
        return new SubMapEntryIterator(absLowest(), absHighFence());
      }
    }

    public Set<MapIV.Entry<V>> entrySet() {
      EntrySetView es = entrySetView;
      return (es != null) ? es : new AscendingEntrySetView();
    }

    TMapIV.Entry<V> subLowest() {
      return absLowest();
    }

    TMapIV.Entry<V> subHighest() {
      return absHighest();
    }

    TMapIV.Entry<V> subCeiling(int key) {
      return absCeiling(key);
    }

    TMapIV.Entry<V> subHigher(int key) {
      return absHigher(key);
    }

    TMapIV.Entry<V> subFloor(int key) {
      return absFloor(key);
    }

    TMapIV.Entry<V> subLower(int key) {
      return absLower(key);
    }

    @Override
    public Collection<V> values() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void clear() {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean containsValue(V value) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void putAll(MapIV<V> m) {
      // TODO Auto-generated method stub

    }
  }

  /**
   * @serial include
   */
  static final class DescendingSubMap<V> extends NavigableSubMap<V> {
    private static final long serialVersionUID = 912986545866120460L;

    DescendingSubMap(TMapIV<V> m, boolean fromStart, int lo, boolean loInclusive, boolean toEnd,
        int hi, boolean hiInclusive) {
      super(m, fromStart, lo, loInclusive, toEnd, hi, hiInclusive);
    }

    public NavigableMapIV<V> subMap(int fromKey, boolean fromInclusive, int toKey,
        boolean toInclusive) {
      if (!inRange(fromKey, fromInclusive))
        throw new IllegalArgumentException("fromKey out of range");
      if (!inRange(toKey, toInclusive))
        throw new IllegalArgumentException("toKey out of range");
      return new DescendingSubMap<V>(m, false, toKey, toInclusive, false, fromKey, fromInclusive);
    }

    public NavigableMapIV<V> headMap(int toKey, boolean inclusive) {
      if (!inRange(toKey, inclusive))
        throw new IllegalArgumentException("toKey out of range");
      return new DescendingSubMap<V>(m, false, toKey, inclusive, toEnd, hi, hiInclusive);
    }

    public NavigableMapIV<V> tailMap(int fromKey, boolean inclusive) {
      if (!inRange(fromKey, inclusive))
        throw new IllegalArgumentException("fromKey out of range");
      return new DescendingSubMap<V>(m, fromStart, lo, loInclusive, false, fromKey, inclusive);
    }

    public NavigableMapIV<V> descendingMap() {
      NavigableMapIV<V> mv = descendingMapView;
      return (mv != null) ? mv : (descendingMapView = new AscendingSubMap<V>(m, fromStart, lo,
          loInclusive, toEnd, hi, hiInclusive));
    }

    Iterator<Integer> keyIterator() {
      return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
    }

    Iterator<Integer> descendingKeyIterator() {
      return new SubMapKeyIterator(absLowest(), absHighFence());
    }

    final class DescendingEntrySetView extends EntrySetView {
      public Iterator<MapIV.Entry<V>> iterator() {
        return new DescendingSubMapEntryIterator(absHighest(), absLowFence());
      }
    }

    public Set<MapIV.Entry<V>> entrySet() {
      EntrySetView es = entrySetView;
      return (es != null) ? es : new DescendingEntrySetView();
    }

    TMapIV.Entry<V> subLowest() {
      return absHighest();
    }

    TMapIV.Entry<V> subHighest() {
      return absLowest();
    }

    TMapIV.Entry<V> subCeiling(int key) {
      return absFloor(key);
    }

    TMapIV.Entry<V> subHigher(int key) {
      return absLower(key);
    }

    TMapIV.Entry<V> subFloor(int key) {
      return absCeiling(key);
    }

    TMapIV.Entry<V> subLower(int key) {
      return absHigher(key);
    }

    @Override
    public Collection<V> values() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void clear() {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean containsValue(V value) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void putAll(MapIV<V> m) {
      // TODO Auto-generated method stub

    }
  }

  // Red-black mechanics

  private static final boolean RED = false;
  private static final boolean BLACK = true;

  /**
   * Node in the Tree. Doubles as a means to pass key-value pairs back to user (see MapIV.Entry).
   */
  static final class Entry<V> implements MapIV.Entry<V> {
    int key;
    V value;
    Entry<V> left = null;
    Entry<V> right = null;
    Entry<V> parent;
    boolean color = BLACK;

    /**
     * Make a new cell with given key, value, and parent, and with <tt>null</tt> child links, and
     * BLACK color.
     */
    Entry(int key, V value, Entry<V> parent) {
      this.key = key;
      this.value = value;
      this.parent = parent;
    }

    /**
     * Returns the key.
     * 
     * @return the key
     */
    public int getKey() {
      return key;
    }

    /**
     * Returns the value associated with the key.
     * 
     * @return the value associated with the key
     */
    public V getValue() {
      return value;
    }

    /**
     * Replaces the value currently associated with the key with the given value.
     * 
     * @return the value associated with the key before this method was called
     */
    public V setValue(V value) {
      V oldValue = this.value;
      this.value = value;
      return oldValue;
    }

    public boolean equals(Object o) {
      if (!(o instanceof MapIV.Entry))
        return false;
      MapIV.Entry<?> e = (MapIV.Entry<?>) o;

      return key == e.getKey() && valEquals(value, e.getValue());
    }

    public int hashCode() {
      int keyHash = key;
      int valueHash = (value == null ? 0 : value.hashCode());
      return keyHash ^ valueHash;
    }

    public String toString() {
      return key + "=" + value;
    }
  }

  /**
   * Returns the first Entry in the TMapIV (according to the TMapIV's key-sort function). Returns
   * null if the TMapIV is empty.
   */
  final Entry<V> getFirstEntry() {
    Entry<V> p = root;
    if (p != null)
      while (p.left != null)
        p = p.left;
    return p;
  }

  /**
   * Returns the last Entry in the TMapIV (according to the TMapIV's key-sort function). Returns
   * null if the TMapIV is empty.
   */
  final Entry<V> getLastEntry() {
    Entry<V> p = root;
    if (p != null)
      while (p.right != null)
        p = p.right;
    return p;
  }

  /**
   * Returns the successor of the specified Entry, or null if no such.
   */
  static <V> TMapIV.Entry<V> successor(Entry<V> t) {
    if (t == null)
      return null;
    else if (t.right != null) {
      Entry<V> p = t.right;
      while (p.left != null)
        p = p.left;
      return p;
    } else {
      Entry<V> p = t.parent;
      Entry<V> ch = t;
      while (p != null && ch == p.right) {
        ch = p;
        p = p.parent;
      }
      return p;
    }
  }

  /**
   * Returns the predecessor of the specified Entry, or null if no such.
   */
  static <V> Entry<V> predecessor(Entry<V> t) {
    if (t == null)
      return null;
    else if (t.left != null) {
      Entry<V> p = t.left;
      while (p.right != null)
        p = p.right;
      return p;
    } else {
      Entry<V> p = t.parent;
      Entry<V> ch = t;
      while (p != null && ch == p.left) {
        ch = p;
        p = p.parent;
      }
      return p;
    }
  }

  /**
   * Balancing operations.
   * 
   * Implementations of rebalancings during insertion and deletion are slightly different than the
   * CLR version. Rather than using dummy nilnodes, we use a set of accessors that deal properly
   * with null. They are used to avoid messiness surrounding nullness checks in the main algorithms.
   */

  private static <V> boolean colorOf(Entry<V> p) {
    return (p == null ? BLACK : p.color);
  }

  private static <V> Entry<V> parentOf(Entry<V> p) {
    return (p == null ? null : p.parent);
  }

  private static <V> void setColor(Entry<V> p, boolean c) {
    if (p != null)
      p.color = c;
  }

  private static <V> Entry<V> leftOf(Entry<V> p) {
    return (p == null) ? null : p.left;
  }

  private static <V> Entry<V> rightOf(Entry<V> p) {
    return (p == null) ? null : p.right;
  }

  /** From CLR */
  private void rotateLeft(Entry<V> p) {
    if (p != null) {
      Entry<V> r = p.right;
      p.right = r.left;
      if (r.left != null)
        r.left.parent = p;
      r.parent = p.parent;
      if (p.parent == null)
        root = r;
      else if (p.parent.left == p)
        p.parent.left = r;
      else
        p.parent.right = r;
      r.left = p;
      p.parent = r;
    }
  }

  /** From CLR */
  private void rotateRight(Entry<V> p) {
    if (p != null) {
      Entry<V> l = p.left;
      p.left = l.right;
      if (l.right != null)
        l.right.parent = p;
      l.parent = p.parent;
      if (p.parent == null)
        root = l;
      else if (p.parent.right == p)
        p.parent.right = l;
      else
        p.parent.left = l;
      l.right = p;
      p.parent = l;
    }
  }

  /** From CLR */
  private void fixAfterInsertion(Entry<V> x) {
    x.color = RED;

    while (x != null && x != root && x.parent.color == RED) {
      if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
        Entry<V> y = rightOf(parentOf(parentOf(x)));
        if (colorOf(y) == RED) {
          setColor(parentOf(x), BLACK);
          setColor(y, BLACK);
          setColor(parentOf(parentOf(x)), RED);
          x = parentOf(parentOf(x));
        } else {
          if (x == rightOf(parentOf(x))) {
            x = parentOf(x);
            rotateLeft(x);
          }
          setColor(parentOf(x), BLACK);
          setColor(parentOf(parentOf(x)), RED);
          rotateRight(parentOf(parentOf(x)));
        }
      } else {
        Entry<V> y = leftOf(parentOf(parentOf(x)));
        if (colorOf(y) == RED) {
          setColor(parentOf(x), BLACK);
          setColor(y, BLACK);
          setColor(parentOf(parentOf(x)), RED);
          x = parentOf(parentOf(x));
        } else {
          if (x == leftOf(parentOf(x))) {
            x = parentOf(x);
            rotateRight(x);
          }
          setColor(parentOf(x), BLACK);
          setColor(parentOf(parentOf(x)), RED);
          rotateLeft(parentOf(parentOf(x)));
        }
      }
    }
    root.color = BLACK;
  }

  /**
   * Delete node p, and then rebalance the tree.
   */
  private void deleteEntry(Entry<V> p) {
    modCount++;
    size--;

    // If strictly internal, copy successor's element to p and then make p
    // point to successor.
    if (p.left != null && p.right != null) {
      Entry<V> s = successor(p);
      p.key = s.key;
      p.value = s.value;
      p = s;
    } // p has 2 children

    // Start fixup at replacement node, if it exists.
    Entry<V> replacement = (p.left != null ? p.left : p.right);

    if (replacement != null) {
      // Link replacement to parent
      replacement.parent = p.parent;
      if (p.parent == null)
        root = replacement;
      else if (p == p.parent.left)
        p.parent.left = replacement;
      else
        p.parent.right = replacement;

      // Null out links so they are OK to use by fixAfterDeletion.
      p.left = p.right = p.parent = null;

      // Fix replacement
      if (p.color == BLACK)
        fixAfterDeletion(replacement);
    } else if (p.parent == null) { // return if we are the only node.
      root = null;
    } else { // No children. Use self as phantom replacement and unlink.
      if (p.color == BLACK)
        fixAfterDeletion(p);

      if (p.parent != null) {
        if (p == p.parent.left)
          p.parent.left = null;
        else if (p == p.parent.right)
          p.parent.right = null;
        p.parent = null;
      }
    }
  }

  /** From CLR */
  private void fixAfterDeletion(Entry<V> x) {
    while (x != root && colorOf(x) == BLACK) {
      if (x == leftOf(parentOf(x))) {
        Entry<V> sib = rightOf(parentOf(x));

        if (colorOf(sib) == RED) {
          setColor(sib, BLACK);
          setColor(parentOf(x), RED);
          rotateLeft(parentOf(x));
          sib = rightOf(parentOf(x));
        }

        if (colorOf(leftOf(sib)) == BLACK && colorOf(rightOf(sib)) == BLACK) {
          setColor(sib, RED);
          x = parentOf(x);
        } else {
          if (colorOf(rightOf(sib)) == BLACK) {
            setColor(leftOf(sib), BLACK);
            setColor(sib, RED);
            rotateRight(sib);
            sib = rightOf(parentOf(x));
          }
          setColor(sib, colorOf(parentOf(x)));
          setColor(parentOf(x), BLACK);
          setColor(rightOf(sib), BLACK);
          rotateLeft(parentOf(x));
          x = root;
        }
      } else { // symmetric
        Entry<V> sib = leftOf(parentOf(x));

        if (colorOf(sib) == RED) {
          setColor(sib, BLACK);
          setColor(parentOf(x), RED);
          rotateRight(parentOf(x));
          sib = leftOf(parentOf(x));
        }

        if (colorOf(rightOf(sib)) == BLACK && colorOf(leftOf(sib)) == BLACK) {
          setColor(sib, RED);
          x = parentOf(x);
        } else {
          if (colorOf(leftOf(sib)) == BLACK) {
            setColor(rightOf(sib), BLACK);
            setColor(sib, RED);
            rotateLeft(sib);
            sib = leftOf(parentOf(x));
          }
          setColor(sib, colorOf(parentOf(x)));
          setColor(parentOf(x), BLACK);
          setColor(leftOf(sib), BLACK);
          rotateRight(parentOf(x));
          x = root;
        }
      }
    }

    setColor(x, BLACK);
  }

  private static final long serialVersionUID = 919286545866124006L;

  /**
   * Save the state of the <tt>TMapIV</tt> instance to a stream (i.e., serialize it).
   * 
   * @serialData The <i>size</i> of the TMapIV (the number of key-value mappings) is emitted (int),
   *             followed by the key (Object) and value (Object) for each key-value mapping
   *             represented by the TMapIV. The key-value mappings are emitted in key-order (as
   *             determined by the TMapIV's Comparator, or by the keys' natural ordering if the
   *             TMapIV has no Comparator).
   */
  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    // Write out the Comparator and any hidden stuff
    s.defaultWriteObject();

    // Write out size (number of Mappings)
    s.writeInt(size);

    // Write out keys and values (alternating)
    for (Iterator<MapIV.Entry<V>> i = entrySet().iterator(); i.hasNext();) {
      MapIV.Entry<V> e = i.next();
      s.writeObject(e.getKey());
      s.writeObject(e.getValue());
    }
  }

  /**
   * Reconstitute the <tt>TMapIV</tt> instance from a stream (i.e., deserialize it).
   */
  private void readObject(final java.io.ObjectInputStream s) throws java.io.IOException,
      ClassNotFoundException {
    // Read in the Comparator and any hidden stuff
    s.defaultReadObject();

    // Read in size
    int size = s.readInt();

    buildFromSorted(size, null, s, null);
  }

  /** Intended to be called only from TreeSet.readObject */
  void readTreeSet(int size, java.io.ObjectInputStream s, V defaultVal) throws java.io.IOException,
      ClassNotFoundException {
    buildFromSorted(size, null, s, defaultVal);
  }

  /** Intended to be called only from TreeSet.addAll */
  void addAllForTreeSet(SortedSet<Integer> set, V defaultVal) {
    try {
      buildFromSorted(set.size(), set.iterator(), null, defaultVal);
    } catch (java.io.IOException cannotHappen) {
    } catch (ClassNotFoundException cannotHappen) {
    }
  }

  /**
   * Linear time tree building algorithm from sorted data. Can accept keys and/or values from
   * iterator or stream. This leads to too many parameters, but seems better than alternatives. The
   * four formats that this method accepts are:
   * 
   * 1) An iterator of Map.Entries. (it != null, defaultVal == null). 2) An iterator of keys. (it !=
   * null, defaultVal != null). 3) A stream of alternating serialized keys and values. (it == null,
   * defaultVal == null). 4) A stream of serialized keys. (it == null, defaultVal != null).
   * 
   * It is assumed that the comparator of the TMapIV is already set prior to calling this method.
   * 
   * @param size
   *          the number of keys (or key-value pairs) to be read from the iterator or stream
   * @param it
   *          If non-null, new entries are created from entries or keys read from this iterator.
   * @param str
   *          If non-null, new entries are created from keys and possibly values read from this
   *          stream in serialized form. Exactly one of it and str should be non-null.
   * @param defaultVal
   *          if non-null, this default value is used for each value in the map. If null, each value
   *          is read from iterator or stream, as described above.
   * @throws IOException
   *           propagated from stream reads. This cannot occur if str is null.
   * @throws ClassNotFoundException
   *           propagated from readObject. This cannot occur if str is null.
   */
  private void buildFromSorted(int size, Iterator it, java.io.ObjectInputStream str, V defaultVal)
      throws java.io.IOException, ClassNotFoundException {
    this.size = size;
    root = buildFromSorted(0, 0, size - 1, computeRedLevel(size), it, str, defaultVal);
  }

  /**
   * Recursive "helper method" that does the real work of the previous method. Identically named
   * parameters have identical definitions. Additional parameters are documented below. It is
   * assumed that the comparator and size fields of the TMapIV are already set prior to calling this
   * method. (It ignores both fields.)
   * 
   * @param level
   *          the current level of tree. Initial call should be 0.
   * @param lo
   *          the first element index of this subtree. Initial should be 0.
   * @param hi
   *          the last element index of this subtree. Initial should be size-1.
   * @param redLevel
   *          the level at which nodes should be red. Must be equal to computeRedLevel for tree of
   *          this size.
   */
  @SuppressWarnings("unchecked")
  private final Entry<V> buildFromSorted(int level, int lo, int hi, int redLevel, Iterator<?> it,
      java.io.ObjectInputStream str, V defaultVal) throws java.io.IOException,
      ClassNotFoundException {
    /*
     * Strategy: The root is the middlemost element. To get to it, we have to first recursively
     * construct the entire left subtree, so as to grab all of its elements. We can then proceed
     * with right subtree.
     * 
     * The lo and hi arguments are the minimum and maximum indices to pull out of the iterator or
     * stream for current subtree. They are not actually indexed, we just proceed sequentially,
     * ensuring that items are extracted in corresponding order.
     */

    if (hi < lo)
      return null;

    int mid = (lo + hi) / 2;

    Entry<V> left = null;
    if (lo < mid)
      left = buildFromSorted(level + 1, lo, mid - 1, redLevel, it, str, defaultVal);

    // extract key and/or value from iterator or stream
    int key;
    V value;
    if (it != null) {
      if (defaultVal == null) {
        MapIV.Entry<V> entry = (MapIV.Entry<V>) it.next();
        key = entry.getKey();
        value = entry.getValue();
      } else {
        key = (int) (Integer) it.next();
        value = defaultVal;
      }
    } else { // use stream
      key = (int) (Integer) str.readObject();
      value = (defaultVal != null ? defaultVal : (V) str.readObject());
    }

    Entry<V> middle = new Entry<V>(key, value, null);

    // color nodes in non-full bottommost level red
    if (level == redLevel)
      middle.color = RED;

    if (left != null) {
      middle.left = left;
      left.parent = middle;
    }

    if (mid < hi) {
      Entry<V> right = buildFromSorted(level + 1, mid + 1, hi, redLevel, it, str, defaultVal);
      middle.right = right;
      right.parent = middle;
    }

    return middle;
  }

  /**
   * Find the level down to which to assign all nodes BLACK. This is the last `full' level of the
   * complete binary tree produced by buildTree. The remaining nodes are colored RED. (This makes a
   * `nice' set of color assignments wrt future insertions.) This level number is computed by
   * finding the number of splits needed to reach the zeroeth node. (The answer is ~lg(N), but in
   * any case must be computed by same quick O(lg(N)) loop.)
   */
  private static int computeRedLevel(int sz) {
    int level = 0;
    for (int m = sz - 1; m >= 0; m = m / 2 - 1)
      level++;
    return level;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }
}
