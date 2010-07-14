

/* Generic definitions */




/* Assertions (useful to generate conditional code) */
/* Current type and class (and size, if applicable) */
/* Value methods */
/* Interfaces (keys) */
/* Interfaces (values) */
/* Abstract implementations (keys) */
/* Abstract implementations (values) */
/* Static containers (keys) */
/* Static containers (values) */
/* Implementations */
/* Synchronized wrappers */
/* Unmodifiable wrappers */
/* Other wrappers */
/* Methods (keys) */
/* Methods (values) */
/* Methods (keys/values) */
/* Methods that have special names depending on keys (but the special names depend on values) */
/* Equality */
/* Object/Reference-only definitions (keys) */
/* Object/Reference-only definitions (values) */
/* Primitive-type-only definitions (values) */
/*		 
 * fastutil: Fast & compact type-specific collections for Java
 *
 * Copyright (C) 2002-2008 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package it.unimi.dsi.fastutil.objects;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.floats.AbstractFloatCollection;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
/** A type-specific hash map with a fast, small-footprint implementation.
 *
 * <P>Instances of this class use a hash table to represent a map. The table is
 * enlarged as needed when new entries are created, but it is <em>never</em> made
 * smaller (even on a {@link #clear()}). A family of {@linkplain #trim() trimming
 * methods} lets you control the size of the table; this is particularly useful
 * if you reuse instances of this class.
 *
 * <P>The enlargement speed is controlled by the <em>growth factor</em>, a
 * positive number. If the growth factor is <var>p</var>, then the table is
 * enlarged each time roughly by a factor 2<sup>p/16</sup>. By default, <var>p</var> is
 * {@link Hash#DEFAULT_GROWTH_FACTOR}, which means that the table is doubled at
 * each enlargement, but one can easily set more or less aggressive policies by
 * calling {@link #growthFactor(int)} (note that the growth factor is <em>not</em> serialized:
 * deserialized tables gets the {@linkplain Hash#DEFAULT_GROWTH_FACTOR default growth factor}).
 *
 * @see Hash
 * @see HashCommon
 */
public class Object2FloatOpenHashMap <K> extends AbstractObject2FloatMap <K> implements java.io.Serializable, Cloneable, Hash {
 /** The array of keys. */
 protected transient K key[];
 /** The array of values. */
 protected transient float value[];
 /** The array of occupancy states. */
 protected transient byte state[];
 /** The acceptable load factor. */
 protected final float f;
 /** Index into the prime list, giving the current table size. */
 protected transient int p;
 /** Threshold after which we rehash. It must be the table size times {@link #f}. */
 protected transient int maxFill;
 /** Number of free entries in the table (may be less than the table size - {@link #count} because of deleted entries). */
 protected transient int free;
 /** Number of entries in the map. */
 protected int count;
 /** Cached set of entries. */
 protected transient volatile FastEntrySet <K> entries;
 /** Cached set of keys. */
 protected transient volatile ObjectSet <K> keys;
 /** Cached collection of values. */
 protected transient volatile FloatCollection values;
 /** The growth factor of the table. The next table size will be <code>{@link Hash#PRIMES}[{@link #p}+growthFactor</code>. */
 protected transient int growthFactor = Hash.DEFAULT_GROWTH_FACTOR;
 public static final long serialVersionUID = -7046029254386353129L;
 private static final boolean ASSERTS = false;
 /** Creates a new hash map.
	 *
	 * The actual table size is the least available prime greater than <code>n</code>/<code>f</code>.
	 *
	 * @param n the expected number of elements in the hash map.
	 * @param f the load factor.
	 * @see Hash#PRIMES
	 */
 @SuppressWarnings("unchecked")
 public Object2FloatOpenHashMap( final int n, final float f ) {
  if ( f <= 0 || f > 1 ) throw new IllegalArgumentException( "Load factor must be greater than 0 and smaller than or equal to 1" );
  if ( n < 0 ) throw new IllegalArgumentException( "Hash table size must be nonnegative" );
  int l = Arrays.binarySearch( PRIMES, (int)( n / f ) + 1 );
  if ( l < 0 ) l = -l - 1;
  free = PRIMES[ p = l ];
  this.f = f;
  this.maxFill = (int)( free * f );
  key = (K[]) new Object[ free ];
  value = new float[ free ];
  state = new byte[ free ];
 }
 /** Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
	 *
	 * @param n the expected number of elements in the hash map.
	 */
 public Object2FloatOpenHashMap( final int n ) {
  this( n, DEFAULT_LOAD_FACTOR );
 }
 /** Creates a new hash map with {@link Hash#DEFAULT_INITIAL_SIZE} entries
	 * and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
	 */
 public Object2FloatOpenHashMap() {
  this( DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR );
 }
 /** Creates a new hash map copying a given one.
	 *
	 * @param m a {@link Map} to be copied into the new hash map. 
	 * @param f the load factor.
	 */
 public Object2FloatOpenHashMap( final Map<? extends K, ? extends Float> m, final float f ) {
  this( m.size(), f );
  putAll( m );
 }
 /** Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given one.
	 *
	 * @param m a {@link Map} to be copied into the new hash map. 
	 */
 public Object2FloatOpenHashMap( final Map<? extends K, ? extends Float> m ) {
  this( m, DEFAULT_LOAD_FACTOR );
 }
 /** Creates a new hash map copying a given type-specific one.
	 *
	 * @param m a type-specific map to be copied into the new hash map. 
	 * @param f the load factor.
	 */
 public Object2FloatOpenHashMap( final Object2FloatMap <K> m, final float f ) {
  this( m.size(), f );
  putAll( m );
 }
 /** Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given type-specific one.
	 *
	 * @param m a type-specific map to be copied into the new hash map. 
	 */
 public Object2FloatOpenHashMap( final Object2FloatMap <K> m ) {
  this( m, DEFAULT_LOAD_FACTOR );
 }
 /** Creates a new hash map using the elements of two parallel arrays.
	 *
	 * @param k the array of keys of the new hash map.
	 * @param v the array of corresponding values in the new hash map.
	 * @param f the load factor.
	 * @throws IllegalArgumentException if <code>k</code> and <code>v</code> have different lengths.
	 */
 public Object2FloatOpenHashMap( final K[] k, final float v[], final float f ) {
  this( k.length, f );
  if ( k.length != v.length ) throw new IllegalArgumentException( "The key array and the value array have different lengths (" + k.length + " and " + v.length + ")" );
  for( int i = 0; i < k.length; i++ ) this.put( k[ i ], v[ i ] );
 }
 /** Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor using the elements of two parallel arrays.
	 *
	 * @param k the array of keys of the new hash map.
	 * @param v the array of corresponding values in the new hash map.
	 * @throws IllegalArgumentException if <code>k</code> and <code>v</code> have different lengths.
	 */
 public Object2FloatOpenHashMap( final K[] k, final float v[] ) {
  this( k, v, DEFAULT_LOAD_FACTOR );
 }
 /** Sets the growth factor. Subsequent enlargements will increase the table
	 * size roughly by a multiplicative factor of 2<sup>p/16</sup>.
	 * 
	 * @param growthFactor the new growth factor; it must be positive.
	 */
 public void growthFactor( int growthFactor ) {
  if ( growthFactor <= 0 ) throw new IllegalArgumentException( "Illegal growth factor " + growthFactor );
  this.growthFactor = growthFactor;
 }
 /** Gets the growth factor.
	 *
	 * @return the growth factor of this set.
	 * @see #growthFactor(int)
	 */
 public int growthFactor() {
  return growthFactor;
 }
 /*
	 * The following methods implements some basic building blocks used by
	 * all accessors.  They are (and should be maintained) identical to those used in HashSet.drv.
	 */
 /** Searches for a key, keeping track of a possible insertion point.
	 *
	 * @param k the key.
	 * @return the index of the correct insertion point, if the key is not found; otherwise,
	 * <var>-i</var>-1, where <var>i</var> is the index of the entry containing the key.
	 */
 protected final int findInsertionPoint( final K k ) {
  final K key[] = this.key;
  final byte state[] = this.state;
  final int n = key.length;
  // First of all, we make the key into a positive integer.
  final int h, k2i = ( h = ( (k) == null ? 0 : (k).hashCode() ) ) & 0x7FFFFFFF;
  // The primary hash, a.k.a. starting point.
  int h1 = k2i % n;
  if ( state[ h1 ] == OCCUPIED && ! ( (key[ h1 ]) == null ? (k) == null : (h) == (key[ h1 ]).hashCode() && (key[ h1 ]).equals(k) ) ) {
   // The secondary hash.
   final int h2 = ( k2i % ( n - 2 ) ) + 1;
   do {
    h1 += h2;
    if ( h1 >= n || h1 < 0 ) h1 -= n;
   } while( state[ h1 ] == OCCUPIED && ! ( (key[ h1 ]) == null ? (k) == null : (h) == (key[ h1 ]).hashCode() && (key[ h1 ]).equals(k) ) ); // There's always a FREE entry.
  }
  if (state[ h1 ] == FREE) return h1;
  if (state[ h1 ] == OCCUPIED) return -h1-1; // Necessarily, KEY_EQUALS_HASH( k, h, key[ h1 ] ).
  /* Tables without deletions will never use code beyond this point. */
  final int i = h1; // Remember first available bucket for later.
  /** See the comments in the documentation of the interface Hash. */
  if ( ASSERTS ) assert state[ h1 ] == REMOVED;
  if ( ! ( (key[ h1 ]) == null ? (k) == null : (h) == (key[ h1 ]).hashCode() && (key[ h1 ]).equals(k) ) ) {
   // The secondary hash.
   final int h2 = ( k2i % ( n - 2 ) ) + 1;
   do {
    h1 += h2;
    if ( h1 >= n || h1 < 0 ) h1 -= n;
   } while( state[ h1 ] != FREE && ! ( (key[ h1 ]) == null ? (k) == null : (h) == (key[ h1 ]).hashCode() && (key[ h1 ]).equals(k) ) );
  }
  return state[ h1 ] == OCCUPIED ? -h1-1 : i; // In the first case, necessarily, KEY_EQUALS_HASH( k, h, key[ h1 ] ).
 }
 /** Searches for a key.
	 *
	 * @param k the key.
	 * @return the index of the entry containing the key, or -1 if the key wasn't found.
	 */
 protected final int findKey( final K k ) {
  final K key[] = this.key;
  final byte state[] = this.state;
  final int n = key.length;
  // First of all, we make the key into a positive integer.
  final int h, k2i = ( h = ( (k) == null ? 0 : (k).hashCode() ) ) & 0x7FFFFFFF;
  // The primary hash, a.k.a. starting point.
  int h1 = k2i % n;
  /** See the comments in the documentation of the interface Hash. */
  if ( state[ h1 ] != FREE && ! ( (key[ h1 ]) == null ? (k) == null : (h) == (key[ h1 ]).hashCode() && (key[ h1 ]).equals(k) ) ) {
   // The secondary hash.
   final int h2 = ( k2i % ( n - 2 ) ) + 1;
   do {
    h1 += h2;
    if ( h1 >= n || h1 < 0 ) h1 -= n;
   } while( state[ h1 ] != FREE && ! ( (key[ h1 ]) == null ? (k) == null : (h) == (key[ h1 ]).hashCode() && (key[ h1 ]).equals(k) ) ); // There's always a FREE entry.
  }
  return state[ h1 ] == OCCUPIED ? h1 : -1; // In the first case, necessarily, KEY_EQUALS_HASH( k, h, key[ h1 ] ).
 }
 public float put(final K k, final float v) {
  final int i = findInsertionPoint( k );
  if (i < 0) {
   final float oldValue = value[-i-1];
   value[-i-1] = v;
   return oldValue;
  }
  if ( state[i] == FREE ) free--;
  state[i] = OCCUPIED;
  key[i] = k;
  value[i] = v;
  if ( ++count >= maxFill ) {
   int newP = Math.min( p + growthFactor, PRIMES.length - 1 );
   // Just to be sure that size changes when p is very small.
   while( PRIMES[ newP ] == PRIMES[ p ] ) newP++;
   rehash( newP ); // Table too filled, let's rehash
  }
  if ( free == 0 ) rehash( p );
  if ( ASSERTS ) checkTable();
  return defRetValue;
 }
 public Float put(final K ok, final Float ov) {
  final float v = ((ov).floatValue());
  final K k = (ok);
  final int i = findInsertionPoint( k );
  if (i < 0) {
   final float oldValue = value[-i-1];
   value[-i-1] = v;
   return (Float.valueOf(oldValue));
  }
  if ( state[i] == FREE ) free--;
  state[i] = OCCUPIED;
  key[i] = k;
  value[i] = v;
  if ( ++count >= maxFill ) rehash( Math.min(p+16, PRIMES.length-1) ); // Table too filled, let's rehash
  if ( free == 0 ) rehash( p );
  if ( ASSERTS ) checkTable();
  return (null);
 }
 public boolean containsValue( final float v ) {
  final float value[] = this.value;
  final byte state[] = this.state;
  int i = 0, j = count;
  while(j-- != 0) {
   while(state[ i ] != OCCUPIED ) i++;
   if ( ( (value[ i ]) == (v) ) ) return true;
   i++;
  }
  return false;
 }
 /* Removes all elements from this map.
	 *
	 * <P>To increase object reuse, this method does not change the table size.
	 * If you want to reduce the table size, you must use {@link #trim()}.
	 *
	 */
 public void clear() {
  if ( free == state.length ) return;
  free = state.length;
  count = 0;
  ByteArrays.fill( state, FREE );
  // We null all object entries so that the garbage collector can do its work.
  ObjectArrays.fill( key, null );
 }
 /** The entry class for a hash map does not record key and value, but
	 * rather the position in the hash table of the corresponding entry. This
	 * is necessary so that calls to {@link java.util.Map.Entry#setValue(Object)} are reflected in
	 * the map */
 private final class MapEntry implements Object2FloatMap.Entry <K>, Map.Entry<K, Float> {
  private int index;
  MapEntry( final int index ) {
   this.index = index;
  }
  public K getKey() {
   return (key[ index ]);
  }
  public Float getValue() {
   return (Float.valueOf(value[ index ]));
  }
  public float getFloatValue() {
   return value[ index ];
  }
  public float setValue( final float v ) {
   final float oldValue = value[ index ];
   value[ index ] = v;
   return oldValue;
  }
  public Float setValue( final Float v ) {
   return (Float.valueOf(setValue( ((v).floatValue()) )));
  }
  @SuppressWarnings("unchecked")
  public boolean equals( final Object o ) {
   if (!(o instanceof Map.Entry)) return false;
   Map.Entry<K, Float> e = (Map.Entry<K, Float>)o;
   return ( (key[ index ]) == null ? ((e.getKey())) == null : (key[ index ]).equals((e.getKey())) ) && ( (value[ index ]) == (((e.getValue()).floatValue())) );
  }
  public int hashCode() {
   return ( (key[ index ]) == null ? 0 : (key[ index ]).hashCode() ) ^ it.unimi.dsi.fastutil.HashCommon.float2int(value[ index ]);
  }
  public String toString() {
   return key[ index ] + "->" + value[ index ];
  }
 }
 /** An iterator over a hash map. */
 private class MapIterator {
  /** The index of the next entry to be returned. */
  int pos = 0;
  /** The index of the last entry that has been returned. */
  int last = -1;
  /** A downward counter measuring how many entries have been returned. */
  int c = count;
  {
   final byte state[] = Object2FloatOpenHashMap.this.state;
   final int n = state.length;
   if ( c != 0 ) while( pos < n && state[ pos ] != OCCUPIED ) pos++;
  }
  public boolean hasNext() {
   return c != 0 && pos < Object2FloatOpenHashMap.this.state.length;
  }
  public int nextEntry() {
   final byte state[] = Object2FloatOpenHashMap.this.state;
   final int n = state.length;
   if ( ! hasNext() ) throw new NoSuchElementException();
   last = pos;
   if ( --c != 0 ) do pos++; while( pos < n && state[ pos ] != OCCUPIED );
   return last;
  }
  @SuppressWarnings("unchecked")
  public void remove() {
   if (last == -1) throw new IllegalStateException();
   state[last] = REMOVED;
   key[last] = (K) HashCommon.REMOVED;
   count--;
  }
  public int skip( final int n ) {
   int i = n;
   while( i-- != 0 && hasNext() ) nextEntry();
   return n - i - 1;
  }
 }
 private class EntryIterator extends MapIterator implements ObjectIterator<Object2FloatMap.Entry <K> > {
  public Object2FloatMap.Entry <K> next() {
   return new MapEntry( nextEntry() );
  }
 }
 private class FastEntryIterator extends MapIterator implements ObjectIterator<Object2FloatMap.Entry <K> > {
  final BasicEntry <K> entry = new BasicEntry <K> ( (null), (0) );
  public BasicEntry <K> next() {
   final int e = nextEntry();
   entry.key = key[ e ];
   entry.value = value[ e ];
   return entry;
  }
 }
 @SuppressWarnings("unchecked")
 public boolean containsKey( Object k ) {
  return findKey( (K) k ) >= 0;
 }
 public int size() {
  return count;
 }
 public boolean isEmpty() {
  return count == 0;
 }
 @SuppressWarnings("unchecked")
 public float getFloat(final Object k) {
  final int i = findKey( (K) k);
  return i < 0 ? defRetValue : value[i];
 }
 @SuppressWarnings("unchecked")
 public float removeFloat(final Object k) {
  final int i = findKey( (K) k );
  if (i < 0) return defRetValue;
  state[i] = REMOVED;
  count--;
  key[i] = (K) HashCommon.REMOVED;
  return value[i];
 }
 @SuppressWarnings("unchecked")
 public Float remove( final Object ok ) {
  final int i = findKey( (K) (ok) );
  if (i < 0) return (null);
  state[i] = REMOVED;
  count--;
  key[i] = (K) HashCommon.REMOVED;
  if ( ASSERTS ) checkTable();
  return (Float.valueOf(value[i]));
 }
 private final class MapEntrySet extends AbstractObjectSet<Object2FloatMap.Entry <K> > implements FastEntrySet <K> {
  public ObjectIterator<Object2FloatMap.Entry <K> > iterator() {
   return new EntryIterator();
  }
  public ObjectIterator<Object2FloatMap.Entry <K> > fastIterator() {
   return new FastEntryIterator();
  }
  @SuppressWarnings("unchecked")
  public boolean contains( final Object o ) {
   if (!(o instanceof Map.Entry)) return false;
   final Map.Entry<K, Float> e = (Map.Entry<K, Float>)o;
   final int i = findKey( (e.getKey()) );
   return i >= 0 && ( (value[ i ]) == (((e.getValue()).floatValue())) );
  }
  @SuppressWarnings("unchecked")
  public boolean remove( final Object o ) {
   if (!(o instanceof Map.Entry)) return false;
   final Map.Entry<K, Float> e = (Map.Entry<K, Float>)o;
   final int i = findKey( (e.getKey()) );
   if ( i >= 0 ) Object2FloatOpenHashMap.this.remove( e.getKey() );
   return i >= 0;
  }
  public int size() {
   return count;
  }
  public void clear() {
   Object2FloatOpenHashMap.this.clear();
  }
 }
 public FastEntrySet <K> object2FloatEntrySet() {
  if ( entries == null ) entries = new MapEntrySet();
  return entries;
 }
 /** An iterator on keys.
	 *
	 * <P>We simply override the {@link java.util.ListIterator#next()}/{@link java.util.ListIterator#previous()} methods
	 * (and possibly their type-specific counterparts) so that they return keys
	 * instead of entries.
	 */
 private final class KeyIterator extends MapIterator implements ObjectIterator <K> {
  public KeyIterator() { super(); }
  public K next() { return key[ nextEntry() ]; }
 }
 private final class KeySet extends AbstractObjectSet <K> {
  public ObjectIterator <K> iterator() {
   return new KeyIterator();
  }
  public int size() {
   return count;
  }
  public boolean contains( Object k ) {
   return containsKey( k );
  }
  public boolean remove( Object k ) {
   int oldCount = count;
   Object2FloatOpenHashMap.this.remove( k );
   return count != oldCount;
  }
  public void clear() {
   Object2FloatOpenHashMap.this.clear();
  }
 }
 public ObjectSet <K> keySet() {
  if ( keys == null ) keys = new KeySet();
  return keys;
 }
 /** An iterator on values.
	 *
	 * <P>We simply override the {@link java.util.ListIterator#next()}/{@link java.util.ListIterator#previous()} methods
	 * (and possibly their type-specific counterparts) so that they return values
	 * instead of entries.
	 */
 private final class ValueIterator extends MapIterator implements FloatIterator {
  public ValueIterator() { super(); }
  public float nextFloat() { return value[ nextEntry() ]; }
  public Float next() { return (Float.valueOf(value[ nextEntry() ])); }
 }
 public FloatCollection values() {
  if ( values == null ) values = new AbstractFloatCollection () {
    public FloatIterator iterator() {
     return new ValueIterator();
    }
    public int size() {
     return count;
    }
    public boolean contains( float v ) {
     return containsValue( v );
    }
    public void clear() {
     Object2FloatOpenHashMap.this.clear();
    }
   };
  return values;
 }
 /** Rehashes this map without changing the table size.
	 * <P>This method should be called when the map underwent numerous deletions and insertions.
	 * In this case, free entries become rare, and unsuccessful searches
	 * require probing <em>all</em> entries. For reasonable load factors this method is linear in the number of entries.
	 * You will need as much additional free memory as
	 * that occupied by the table.
	 *
	 * <P>If you need to reduce the table siza to fit exactly
	 * this map, you must use {@link #trim()}.
	 *
	 * @return <code>true</code> if there was enough memory to rehash the map, <code>false</code> otherwise.
	 * @see #trim()
	 */
 public boolean rehash() {
  try {
   rehash(p);
  }
  catch(OutOfMemoryError cantDoIt) { return false; }
  return true;
 }
 /** Rehashes the map, making the table as small as possible.
	 * 
	 * <P>This method rehashes to the smallest size satisfying
	 * the load factor. It can be used when the map will not be
	 * changed anymore, so to optimize access speed (by collecting
	 * deleted entries) and size.
	 *
	 * <P>If the table size is already the minimum possible, this method
	 * does nothing. If you want to guarantee rehashing, use {@link #rehash()}.
	 *
	 * @return true if there was enough memory to trim the map.
	 * @see #trim(int)
	 * @see #rehash()
	 */
 public boolean trim() {
  int l = Arrays.binarySearch( PRIMES, (int)( count / f ) + 1 );
  if ( l < 0 ) l = -l - 1;
  if ( l >= p ) return true;
  try {
   rehash( l );
  }
  catch(OutOfMemoryError cantDoIt) { return false; }
  return true;
 }
 /** Rehashes this map if the table is too large.
	 * 
	 * <P>Let <var>N</var> be the smallest table size that can hold
	 * <code>max(n,{@link #size()})</code> entries, still satisfying the load factor. If the current
	 * table size is smaller than or equal to <var>N</var>, this method does
	 * nothing. Otherwise, it rehashes this map in a table of size
	 * <var>N</var>.
	 *
	 * <P>This method is useful when reusing maps.  {@linkplain #clear() Clearing a
	 * map} leaves the table size untouched. If you are reusing a map
	 * many times, you can call this method with a typical
	 * size to avoid keeping around a very large table just
	 * because of a few large transient maps.
	 *
	 * @param n the threshold for the trimming.
	 * @return true if there was enough memory to trim the map.
	 * @see #trim()
	 * @see #rehash()
	 */
 public boolean trim( final int n ) {
  int l = Arrays.binarySearch( PRIMES, (int)( Math.min( Integer.MAX_VALUE - 1, Math.max( n, count ) / f ) ) + 1 );
  if ( l < 0 ) l = -l - 1;
  if ( p <= l ) return true;
  try {
   rehash( l );
  }
  catch( OutOfMemoryError cantDoIt ) { return false; }
  return true;
 }
 /** Resizes the map.
	 *
	 * <P>This method implements the basic rehashing strategy, and may be
	 * overriden by subclasses implementing different rehashing strategies (e.g.,
	 * disk-based rehashing). However, you should not override this method
	 * unless you understand the internal workings of this class.
	 *
	 * @param newP the new size as an index in {@link Hash#PRIMES}.
	 */
 @SuppressWarnings("unchecked")
 protected void rehash( final int newP ) {
  int i = 0, j = count, k2i, h1, h2;
  final byte state[] = this.state;
  K k;
  float v;
  final int newN = PRIMES[newP];
  final K key[] = this.key, newKey[] = (K[]) new Object[newN];
  final float value[] = this.value, newValue[] = new float[newN];
  final byte newState[] = new byte[newN];
  while(j-- != 0) {
   while(state[i] != OCCUPIED ) i++;
   k = key[i];
   v = value[i];
   k2i = ( (k) == null ? 0 : (k).hashCode() ) & 0x7FFFFFFF;
   h1 = k2i % newN;
   h2 = (k2i % (newN - 2)) + 1;
   if ( newState[h1] != FREE ) {
    h2 = (k2i % (newN - 2)) + 1;
    do {
     h1 += h2;
     if ( h1 >= newN || h1 < 0 ) h1 -= newN;
    } while( newState[h1] != FREE );
   }
   newState[h1] = OCCUPIED;
   newKey[h1] = k;
   newValue[h1] = v;
   i++;
  }
  p = newP;
  free = newN - count;
  maxFill = (int)( newN * f );
  this.key = newKey;
  this.value = newValue;
  this.state = newState;
 }
 /** Returns a deep copy of this map. 
	 *
	 * <P>This method performs a deep copy of this hash map; the data stored in the
	 * map, however, is not cloned. Note that this makes a difference only for object keys.
	 *
	 *  @return a deep copy of this map.
	 */
 @SuppressWarnings("unchecked")
 public Object clone() {
  Object2FloatOpenHashMap c;
  try {
   c = (Object2FloatOpenHashMap)super.clone();
  }
  catch(CloneNotSupportedException cantHappen) {
   throw new InternalError();
  }
  c.keys = null;
  c.values = null;
  c.entries = null;
  c.key = key.clone();
  c.value = value.clone();
  c.state = state.clone();
  return c;
 }
 /** Returns a hash code for this map.
	 *
	 * This method overrides the generic method provided by the superclass. 
	 * Since <code>equals()</code> is not overriden, it is important
	 * that the value returned by this method is the same value as
	 * the one returned by the overriden method.
	 *
	 * @return a hash code for this map.
	 */
 public int hashCode() {
  int h = 0, t, i = 0, j = count;
  while( j-- != 0 ) {
   while( state[ i ] != OCCUPIED ) i++;
   t = 0;
   if ( this != key[ i ] )
    t = ( (key[ i ]) == null ? 0 : (key[ i ]).hashCode() );
    t ^= it.unimi.dsi.fastutil.HashCommon.float2int(value[ i ]);
   h += t;
   i++;
  }
  return h;
 }
 private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
  final K key[] = this.key;
  final float value[] = this.value;
  final MapIterator i = new MapIterator();
  int e, j = count;
  s.defaultWriteObject();
  while( j-- != 0 ) {
   e = i.nextEntry();
   s.writeObject( key[ e ] );
   s.writeFloat( value[ e ] );
  }
 }
 @SuppressWarnings("unchecked")
 private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
  s.defaultReadObject();
  // We restore the default growth factor.
  growthFactor = Hash.DEFAULT_GROWTH_FACTOR;
  // Note that we DO NOT USE the stored p. See CHANGES.
  p = Arrays.binarySearch( PRIMES, (int)( count / f ) + 1 );
  if ( p < 0 ) p = -p - 1;
  final int n = PRIMES[ p ];
  maxFill = (int)( n * f );
  free = n - count;;
  final K key[] = this.key = (K[]) new Object[ n ];
  final float value[] = this.value = new float[ n ];
  final byte state[] = this.state = new byte[ n ];
  int i, k2i, h1, h2;
  K k;
  float v;
  i = count;
  while( i-- != 0 ) {
   k = (K) s.readObject();
   v = s.readFloat();
   k2i = ( (k) == null ? 0 : (k).hashCode() ) & 0x7FFFFFFF;
   h1 = k2i % n;
   if ( state[ h1 ] != FREE ) {
    h2 = ( k2i % ( n - 2 ) ) + 1;
    do {
     h1 += h2;
     if ( h1 >= n || h1 < 0 ) h1 -= n;
    } while( state[ h1 ] != FREE );
   }
   state[ h1 ] = OCCUPIED;
   key[ h1 ] = k;
   value[ h1 ] = v;
  }
  if ( ASSERTS ) checkTable();
 }
 private void checkTable() {}
}
