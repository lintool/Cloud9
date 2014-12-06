/*
 * @(#)Map.java 1.56 06/04/21
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package edu.umd.cloud9.util.map;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Map from arbitrary objects to shorts.
 *
 * @param <K> type of the keys
 */
public interface MapKS<K> {
  public static final short DEFAULT_VALUE = 0;

  // Query Operations

  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return the number of key-value mappings in this map
   */
  int size();

  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   *
   * @return <tt>true</tt> if this map contains no key-value mappings
   */
  boolean isEmpty();

  /**
   * Returns <tt>true</tt> if this map contains a mapping for the specified key.
   *
   * @param key key whose presence in this map is to be tested
   * @return <tt>true</tt> if this map contains a mapping for the specified key
   */
  boolean containsKey(K key);

  /**
   * Returns <tt>true</tt> if this map contains one or more mappings with the specified value.
   *
   * @param value value whose presence in this map is to be tested
   * @return <tt>true</tt> this map contains one or more mappings with the specified value
   */
  boolean containsValue(short value);

  /**
   * Returns the value to which the specified key is mapped, or throws
   * {@link NoSuchElementException} if this map contains no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped
   * @throws NoSuchElementException if the key is not contained in this map
   */
  short get(K key);

  // Modification Operations

  /**
   * Associates the specified value with the specified key in this map. If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value of the key (or the default value, if none)
   */
  short put(K key, short value);

  /**
   * Removes the mapping for a key from this map if it is present. No action is performed if this
   * map does not contain the key.
   *
   * @param key key whose mapping is to be removed from the map
   */
  short remove(K key);

  // Bulk Operations

  /**
   * Copies all of the mappings from the specified map to this map.
   *
   * @param m mappings to be stored in this map
   */
  void putAll(MapKS<? extends K> m);

  /**
   * Removes all of the mappings from this map. The map will be empty after this call returns.
   */
  void clear();

  // Views

  /**
   * Returns a {@link Set} view of the keys contained in this map. The set is backed by the map, so
   * changes to the map are reflected in the set, and vice-versa.
   *
   * @return a set view of the keys contained in this map
   */
  Set<K> keySet();

  /**
   * Returns a {@link Collection} view of the values contained in this map. Note that this is a
   * inefficient operation since it triggers autoboxing of the short values, which is exactly what
   * this implementation is trying to avoid. Unlike a standard Java <tt>Map</tt>, values in the
   * backing map cannot be altered with this collection view.
   *
   * @return a collection view of the values contained in this map
   */
  Collection<Short> values();

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set is backed by the map,
   * so changes to the map are reflected in the set, and vice-versa.
   *
   * @return a set view of the mappings contained in this map
   */
  Set<MapKS.Entry<K>> entrySet();

  /**
   * A map entry (key-value pair) for <tt>MapKS</tt>. The <tt>MapKS.entrySet</tt> method returns a
   * collection-view of the map, whose elements are of this class. The <i>only</i> way to obtain a
   * reference to a map entry is from the iterator of this collection-view. These
   * <tt>MapKS.Entry</tt> objects are valid <i>only</i> for the duration of the iteration; more
   * formally, the behavior of a map entry is undefined if the backing map has been modified after
   * the entry was returned by the iterator, except through the <tt>setValue</tt> operation on the
   * map entry.
   */
  interface Entry<K> {
    /**
     * Returns the key corresponding to this entry.
     *
     * @return the key corresponding to this entry
     */
    K getKey();

    /**
     * Returns the value corresponding to this entry. If the mapping has been removed from the
     * backing map (by the iterator's <tt>remove</tt> operation), the results of this call are
     * undefined.
     *
     * @return the value corresponding to this entry
     */
    short getValue();

    /**
     * Replaces the value corresponding to this entry with the specified value, and write through to
     * the backing map. The behavior of this call is undefined if the mapping has already been
     * removed from the map (by the iterator's <tt>remove</tt> operation).
     *
     * @param value new value to be stored in this entry
     * @return old value corresponding to the entry
     */
    short setValue(short value);

    /**
     * Compares the specified object with this entry for equality. Returns <tt>true</tt> if the
     * given object is also a map entry and the two entries represent the same mapping.
     *
     * @param o object to be compared for equality with this map entry
     * @return <tt>true</tt> if the specified object is equal to this map entry
     */
    boolean equals(Object o);

    /**
     * Returns the hash code value for this map entry.
     *
     * @return the hash code value for this map entry
     */
    int hashCode();
  }

  // Comparison and hashing

  /**
   * Compares the specified object with this map for equality. Returns <tt>true</tt> if the given
   * object is also a map and the two maps represent the same mappings. More formally, two maps
   * <tt>m1</tt> and <tt>m2</tt> represent the same mappings if
   * <tt>m1.entrySet().equals(m2.entrySet())</tt>. This ensures that the <tt>equals</tt> method
   * works properly across different implementations of the <tt>MapKS</tt> interface.
   *
   * @param o object to be compared for equality with this map
   * @return <tt>true</tt> if the specified object is equal to this map
   */
  boolean equals(Object o);

  /**
   * Returns the hash code value for this map. The hash code of a map is defined to be the sum of
   * the hash codes of each entry in the map's <tt>entrySet()</tt> view. This ensures that
   * <tt>m1.equals(m2)</tt> implies that <tt>m1.hashCode()==m2.hashCode()</tt> for any two maps
   * <tt>m1</tt> and <tt>m2</tt>, as required by the general contract of {@link Object#hashCode}.
   *
   * @return the hash code value for this map
   */
  int hashCode();
}
