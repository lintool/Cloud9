

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
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.AbstractObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Iterator;
import java.util.Map;
/** An abstract class providing basic methods for maps implementing a type-specific interface.
 *
 * <P>Optional operations just throw an {@link
 * UnsupportedOperationException}. Generic versions of accessors delegate to
 * the corresponding type-specific counterparts following the interface rules
 * (they take care of returning <code>null</code> on a missing key).
 *
 * <P>As a further help, this class provides a {@link BasicEntry BasicEntry} inner class
 * that implements a type-specific version of {@link java.util.Map.Entry}; it
 * is particularly useful for those classes that do not implement their own
 * entries (e.g., most immutable maps).
 */
public abstract class AbstractObject2ObjectMap <K,V> extends AbstractObject2ObjectFunction <K,V> implements Object2ObjectMap <K,V>, java.io.Serializable {
 public static final long serialVersionUID = -4940583368468432370L;
 protected AbstractObject2ObjectMap() {}
 /** Checks whether the given value is contained in {@link #values()}. */
 public boolean containsValue( Object v ) {
  return values().contains( v );
 }
 /** Checks whether the given value is contained in {@link #keySet()}. */
 public boolean containsKey( Object k ) {
  return keySet().contains( k );
 }
 /** Puts all pairs in the given map.
	 * If the map implements the interface of this map,
	 * it uses the faster iterators.
	 *
	 * @param m a map.
	 */
 @SuppressWarnings("unchecked")
 public void putAll(Map<? extends K,? extends V> m) {
  int n = m.size();
  final Iterator<? extends Map.Entry<? extends K,? extends V>> i = m.entrySet().iterator();
  if (m instanceof Object2ObjectMap) {
   Object2ObjectMap.Entry <? extends K, ? extends V> e;
   while(n-- != 0) {
    e = (Object2ObjectMap.Entry <? extends K, ? extends V>)i.next();
    put(e.getKey(), e.getValue());
   }
  }
  else {
   Map.Entry<? extends K,? extends V> e;
   while(n-- != 0) {
    e = i.next();
    put(e.getKey(), e.getValue());
   }
  }
 }
 public boolean isEmpty() {
  return size() == 0;
 }
 /** This class provides a basic but complete type-specific entry class for all those maps implementations
	 * that do not have entries on their own (e.g., most immutable maps). 
	 *
	 * <P>This class does not implement {@link java.util.Map.Entry#setValue(Object) setValue()}, as the modification
	 * would not be reflected in the base map.
	 */
 public static class BasicEntry <K,V> implements Object2ObjectMap.Entry <K,V> {
  protected K key;
  protected V value;

  public BasicEntry( final K key, final V value ) {
   this.key = (key);
   this.value = (value);
  }
  public K getKey() {
   return (key);
  }
  public V getValue() {
   return (value);
  }
  public V setValue( final V value ) {
   throw new UnsupportedOperationException();
  }
  public boolean equals( final Object o ) {
   if (!(o instanceof Map.Entry)) return false;
   Map.Entry<?,?> e = (Map.Entry<?,?>)o;
   return ( (key) == null ? ((e.getKey())) == null : (key).equals((e.getKey())) ) && ( (value) == null ? ((e.getValue())) == null : (value).equals((e.getValue())) );
  }
  public int hashCode() {
   return ( (key) == null ? 0 : (key).hashCode() ) ^ ( (value) == null ? 0 : (value).hashCode() );
  }
  public String toString() {
   return key + "->" + value;
  }
 }
 /** Returns a type-specific-set view of the keys of this map.
	 *
	 * <P>The view is backed by the set returned by {@link #entrySet()}. Note that
	 * <em>no attempt is made at caching the result of this method</em>, as this would
	 * require adding some attributes that lightweight implementations would
	 * not need. Subclasses may easily override this policy by calling
	 * this method and caching the result, but implementors are encouraged to
	 * write more efficient ad-hoc implementations.
	 *
	 * @return a set view of the keys of this map; it may be safely cast to a type-specific interface.
	 */
 public ObjectSet <K> keySet() {
  return new AbstractObjectSet <K>() {
    public boolean contains( final Object k ) { return containsKey( k ); }
    public int size() { return AbstractObject2ObjectMap.this.size(); }
    public void clear() { AbstractObject2ObjectMap.this.clear(); }
    public ObjectIterator <K> iterator() {
     return new AbstractObjectIterator <K>() {
       final ObjectIterator<Map.Entry<K,V>> i = entrySet().iterator();
       public K next() { return ((Object2ObjectMap.Entry <K,V>)i.next()).getKey(); };
       public boolean hasNext() { return i.hasNext(); }
      };
    }
   };
 }
 /** Returns a type-specific-set view of the values of this map.
	 *
	 * <P>The view is backed by the set returned by {@link #entrySet()}. Note that
	 * <em>no attempt is made at caching the result of this method</em>, as this would
	 * require adding some attributes that lightweight implementations would
	 * not need. Subclasses may easily override this policy by calling
	 * this method and caching the result, but implementors are encouraged to
	 * write more efficient ad-hoc implementations.
	 *
	 * @return a set view of the values of this map; it may be safely cast to a type-specific interface.
	 */
 public ObjectCollection <V> values() {
  return new AbstractObjectCollection <V>() {
    public boolean contains( final Object k ) { return containsValue( k ); }
    public int size() { return AbstractObject2ObjectMap.this.size(); }
    public void clear() { AbstractObject2ObjectMap.this.clear(); }
    public ObjectIterator <V> iterator() {
     return new AbstractObjectIterator <V>() {
       final ObjectIterator<Map.Entry<K,V>> i = entrySet().iterator();
       public V next() { return ((Object2ObjectMap.Entry <K,V>)i.next()).getValue(); };
       public boolean hasNext() { return i.hasNext(); }
      };
    }
   };
 }
 @SuppressWarnings("unchecked")
 public ObjectSet<Map.Entry<K, V>> entrySet() {
  return (ObjectSet)object2ObjectEntrySet();
 }
 /** Returns a hash code for this map.
	 *
	 * The hash code of a map is computed by summing the hash codes of its entries.
	 *
	 * @return a hash code for this map.
	 */
 public int hashCode() {
  int h = 0, n = size();
  final ObjectIterator<? extends Map.Entry<K,V>> i = entrySet().iterator();
  while( n-- != 0 ) h += i.next().hashCode();
  return h;
 }
 public boolean equals(Object o) {
  if ( o == this ) return true;
  if ( ! ( o instanceof Map ) ) return false;
  Map<?,?> m = (Map<?,?>)o;
  if ( m.size() != size() ) return false;
  return entrySet().containsAll( m.entrySet() );
 }
 public String toString() {
  final StringBuilder s = new StringBuilder();
  final ObjectIterator<? extends Map.Entry<K,V>> i = entrySet().iterator();
  int n = size();
  Object2ObjectMap.Entry <K,V> e;
  boolean first = true;
  s.append("{");
  while(n-- != 0) {
   if (first) first = false;
   else s.append(", ");
   e = (Object2ObjectMap.Entry <K,V>)i.next();
   if (this == e.getKey()) s.append("(this map)"); else
    s.append(String.valueOf(e.getKey()));
   s.append("=>");
   if (this == e.getValue()) s.append("(this map)"); else
    s.append(String.valueOf(e.getValue()));
  }
  s.append("}");
  return s.toString();
 }
}
