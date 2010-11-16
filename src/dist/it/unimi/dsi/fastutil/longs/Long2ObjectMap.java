

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
/* Primitive-type-only definitions (keys) */
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
package it.unimi.dsi.fastutil.longs;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Map;
/** A type-specific {@link Map}; provides some additional methods that use polymorphism to avoid (un)boxing, and handling of a default return value.
 *
 * <P>Besides extending the corresponding type-specific {@linkplain it.unimi.dsi.fastutil.Function function}, this interface strengthens {@link #entrySet()},
 * {@link #keySet()} and {@link #values()}. Maps returning entry sets of type {@link FastEntrySet} support also fast iteration.
 *
 * <P>A submap or subset may or may not have an
 * independent default return value (which however must be initialized to the
 * default return value of the originator).
 *
 * @see Map
 */
public interface Long2ObjectMap <V> extends Long2ObjectFunction <V>, Map<Long,V> {
 /** An entry set providing fast iteration. 
	 *
	 * <p>In some cases (e.g., hash-based classes) iteration over an entry set requires the creation
	 * of a large number of {@link Entry} objects. Some <code>fastutil</code>
	 * maps might return {@linkplain #entrySet() entry set} objects of type {@link FastEntrySet FastEntrySet}: in this case, {@link #fastIterator() fastIterator()}
	 * will return an iterator that is guaranteed not to create a large number of objects, <em>possibly
	 * by returning always the same entry</em> (of course, mutated).
	 */
 public interface FastEntrySet <V> extends ObjectSet<Long2ObjectMap.Entry <V> > {
  /** Returns a fast iterator over this entry set; the iterator might return always the same entry object, suitably mutated.
		 *
		 * @return a fast iterator over this entry set; the iterator might return always the same {@link Entry} object, suitably mutated.
		 */
  public ObjectIterator<Long2ObjectMap.Entry <V> > fastIterator();
 }
 /** Returns a set view of the mappings contained in this map.
	 *  <P>Note that this specification strengthens the one given in {@link Map#entrySet()}.
	 *
	 * @return a set view of the mappings contained in this map.
	 * @see Map#entrySet()
	 */
 ObjectSet<Map.Entry<Long, V>> entrySet();
 /** Returns a type-specific set view of the mappings contained in this map.
	 *
	 * <p>This method is necessary because there is no inheritance along
	 * type parameters: it is thus impossible to strengthen {@link #entrySet()}
	 * so that it returns an {@link it.unimi.dsi.fastutil.objects.ObjectSet}
	 * of objects of type {@link Entry} (the latter makes it possible to
	 * access keys and values with type-specific methods).
	 *
	 * @return a type-specific set view of the mappings contained in this map.
	 * @see #entrySet()
	 */
 ObjectSet<Long2ObjectMap.Entry <V> > long2ObjectEntrySet();
 /** Returns a set view of the keys contained in this map.
	 *  <P>Note that this specification strengthens the one given in {@link Map#keySet()}.
	 *
	 * @return a set view of the keys contained in this map.
	 * @see Map#keySet()
	 */
 LongSet keySet();
 /** Returns a set view of the values contained in this map.
	 *  <P>Note that this specification strengthens the one given in {@link Map#values()}.
	 *
	 * @return a set view of the values contained in this map.
	 * @see Map#values()
	 */
 ObjectCollection <V> values();


 /** A type-specific {@link java.util.Map.Entry}; provides some additional methods
	 *  that use polymorphism to avoid (un)boxing.
	 *
	 * @see java.util.Map.Entry
	 */

 interface Entry <V> extends Map.Entry <Long,V> {


  /**
		 * @see java.util.Map.Entry#getKey()
		 */
  long getLongKey();
 }
}
