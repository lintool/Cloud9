

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
import it.unimi.dsi.fastutil.Function;
/** A type-specific {@link Function}; provides some additional methods that use polymorphism to avoid (un)boxing.
 *
 * <P>Type-specific versions of <code>get()</code>, <code>put()</code> and
 * <code>remove()</code> cannot rely on <code>null</code> to denote absence of
 * a key.  Rather, they return a {@linkplain #defaultReturnValue() default
 * return value}, which is set to 0 cast to the return type (<code>false</code>
 * for booleans) at creation, but can be changed using the
 * <code>defaultReturnValue()</code> method. 
 *
 * <P>For uniformity reasons, even maps returning objects implement the default
 * return value (of course, in this case the default return value is
 * initialized to <code>null</code>).
 *
 * <P><strong>Warning:</strong> to fall in line as much as possible with the
 * {@linkplain java.util.Map standard map interface}, it is strongly suggested
 * that standard versions of <code>get()</code>, <code>put()</code> and
 * <code>remove()</code> for maps with primitive-type values <em>return
 * <code>null</code> to denote missing keys</em> rather than wrap the default
 * return value in an object (of course, for maps with object keys and values
 * this is not possible, as there is no type-specific version).
 *
 * @see Function
 */
public interface Object2FloatFunction <K> extends Function<K, Float> {
 /** Adds a pair to the map.
	 *
	 * @param key the key.
	 * @param value the value.
	 * @return the old value, or the {@linkplain #defaultReturnValue() default return value} if no value was present for the given key.
	 * @see Function#put(Object,Object)
	 */
 float put( K key, float value );
 /** Returns the value to which the given key is mapped.
	 *
	 * @param key the key.
	 * @return the corresponding value, or the {@linkplain #defaultReturnValue() default return value} if no value was present for the given key.
	 * @see Function#get(Object)
	 */
 float getFloat( Object key );
 /** Removes the mapping with the given key.
	 * @param key
	 * @return the old value, or the {@linkplain #defaultReturnValue() default return value} if no value was present for the given key.
	 * @see Function#remove(Object)
	 */
 float removeFloat( Object key );
 /** Sets the default return value. 
	 *
	 * This value must be returned by type-specific versions of
	 * <code>get()</code>, <code>put()</code> and <code>remove()</code> to
	 * denote that the map does not contain the specified key. It must be
	 * 0/<code>false</code>/<code>null</code> by default.
	 *
	 * @param rv the new default return value.
	 * @see #defaultReturnValue()
	 */
 void defaultReturnValue( float rv );

 /** Gets the default return value.
	 *
	 * @return the current default return value.
	 */

 float defaultReturnValue();

}
