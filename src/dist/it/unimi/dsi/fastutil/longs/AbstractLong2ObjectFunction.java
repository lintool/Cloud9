

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
/** An abstract class providing basic methods for functions implementing a type-specific interface.
 *
 * <P>Optional operations just throw an {@link
 * UnsupportedOperationException}. Generic versions of accessors delegate to
 * the corresponding type-specific counterparts following the interface rules
 * (they take care of returning <code>null</code> on a missing key).
 *
 * <P>This class handles directly a default return
 * value (including {@linkplain #defaultReturnValue() methods to access
 * it}). Instances of classes inheriting from this class have just to return
 * <code>defRetValue</code> to denote lack of a key in type-specific methods. The value
 * is serialized.
 *
 * <P>Implementing subclasses have just to provide type-specific <code>get()</code>,
 * type-specific <code>containsKey()</code>, and <code>size()</cose> methods.
 *
 */
public abstract class AbstractLong2ObjectFunction <V> implements Long2ObjectFunction <V>, java.io.Serializable {
 protected AbstractLong2ObjectFunction() {}
 /**
	 * The default return value for <code>get()</code>, <code>put()</code> and
	 * <code>remove()</code>.  
	 */
 protected V defRetValue;
 public void defaultReturnValue( final V rv ) {
  defRetValue = rv;
 }
 public V defaultReturnValue() {
  return defRetValue;
 }
 public V put( long key, V value ) {
  throw new UnsupportedOperationException();
 }
 public V remove( long key ) {
  throw new UnsupportedOperationException();
 }
 public void clear() {
  throw new UnsupportedOperationException();
 }
 public boolean containsKey( final Object ok ) {
  return containsKey( ((((Long)(ok)).longValue())) );
 }
 /** Delegates to the corresponding type-specific method, taking care of returning <code>null</code> on a missing key.
	 *
	 * <P>This method must check whether the provided key is in the map using <code>containsKey()</code>. Thus,
	 * it probes the map <em>twice</em>. Implementors of subclasses should override it with a more efficient method.
	 */
 public V get( final Object ok ) {
  final long k = ((((Long)(ok)).longValue()));
  return containsKey( k ) ? (get( k )) : null;
 }
 /** Delegates to the corresponding type-specific method, taking care of returning <code>null</code> on a missing key. 
	 *
	 * <P>This method must check whether the provided key is in the map using <code>containsKey()</code>. Thus,
	 * it probes the map <em>twice</em>. Implementors of subclasses should override it with a more efficient method.
	 */
 public V put( final Long ok, final V ov ) {
  final long k = ((ok).longValue());
  final V v = put( k, (ov) );
  return containsKey( k ) ? (v) : null;
 }
 /** Delegates to the corresponding type-specific method, taking care of returning <code>null</code> on a missing key. 
	 *
	 * <P>This method must check whether the provided key is in the map using <code>containsKey()</code>. Thus,
	 * it probes the map <em>twice</em>. Implementors of subclasses should override it with a more efficient method.
	 */
 public V remove( final Object ok ) {
  final long k = ((((Long)(ok)).longValue()));
  final V v = remove( k );
  return containsKey( k ) ? (v) : null;
 }
}
