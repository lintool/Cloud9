

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
package it.unimi.dsi.fastutil.floats;
import java.util.Collection;
import java.util.Iterator;
/** An abstract class providing basic methods for collections implementing a type-specific interface.
 *
 * <P>In particular, this class provide {@link #iterator()}, <code>add()</code>, {@link #remove(Object)} and
 * {@link #contains(Object)} methods that just call the type-specific counterpart. 
 */
public abstract class AbstractFloatCollection implements FloatCollection {
 protected AbstractFloatCollection() {}
 public float[] toArray( float a[] ) {
  return toFloatArray( a );
 }
 public float[] toFloatArray() {
  return toFloatArray( null );
 }
 public float[] toFloatArray( float a[] ) {
  if ( a == null || a.length < size() ) a = new float[ size() ];
  FloatIterators.unwrap( iterator(), a );
  return a;
 }
 public boolean rem( float k ) {
  throw new UnsupportedOperationException();
 }
 /** Adds all elements of the given type-specific collection to this collection.
	 *
	 * @param c a type-specific collection.
	 * @return <code>true</code> if this collection changed as a result of the call.
	 */
 public boolean addAll( FloatCollection c ) {
  boolean retVal = false;
  final FloatIterator i = c.iterator();
  int n = c.size();
  while( n-- != 0 ) if ( add( i.nextFloat() ) ) retVal = true;
  return retVal;
 }
 /** Checks whether this collection contains all elements from the given type-specific collection.
	 *
	 * @param c a type-specific collection.
	 * @return <code>true</code> if this collection contains all elements of the argument.
	 */
 public boolean containsAll( FloatCollection c ) {
  final FloatIterator i = c.iterator();
  int n = c.size();
  while( n-- != 0 ) if ( ! contains( i.nextFloat() ) ) return false;
  return true;
 }
 /** Retains in this collection only elements from the given type-specific collection.
	 *
	 * @param c a type-specific collection.
	 * @return <code>true</code> if this collection changed as a result of the call.
	 */
 public boolean retainAll( FloatCollection c ) {
  boolean retVal = false;
  int n = size();
  final FloatIterator i = iterator();
  while( n-- != 0 ) {
   if ( ! c.contains( i.nextFloat() ) ) {
    i.remove();
    retVal = true;
   }
  }
  return retVal;
 }
 /** Remove from this collection all elements in the given type-specific collection.
	 *
	 * @param c a type-specific collection.
	 * @return <code>true</code> if this collection changed as a result of the call.
	 */
 public boolean removeAll( FloatCollection c ) {
  boolean retVal = false;
  int n = c.size();
  final FloatIterator i = c.iterator();

  while( n-- != 0 ) if ( rem( i.nextFloat() ) ) retVal = true;

  return retVal;
 }



 public Object[] toArray() {
  final Object[] a = new Object[ size() ];
  it.unimi.dsi.fastutil.objects.ObjectIterators.unwrap( iterator(), a );
  return a;
 }

 @SuppressWarnings("unchecked")
 public <T> T[] toArray( T[] a ) {
  if ( a.length < size() ) a = (T[])java.lang.reflect.Array.newInstance( a.getClass().getComponentType(), size() );
  it.unimi.dsi.fastutil.objects.ObjectIterators.unwrap( iterator(), a );
  return a;
 }

 /** Adds all elements of the given collection to this collection.
	 *
	 * @param c a collection.
	 * @return <code>true</code> if this collection changed as a result of the call.
	 */

 public boolean addAll( Collection<? extends Float> c ) {
  boolean retVal = false;
  final Iterator<? extends Float> i = c.iterator();
  int n = c.size();

  while( n-- != 0 ) if ( add( i.next() ) ) retVal = true;
  return retVal;
 }



 public boolean add( float k ) {
  throw new UnsupportedOperationException();
 }

 public boolean remove( Object ok ) {

  return rem( ((((Float)(ok)).floatValue())) );



 }

 public void clear() {
  throw new UnsupportedOperationException();
 }

 /** Delegates to the new covariantly stronger generic method. */

 @Deprecated
 public FloatIterator floatIterator() {
  return iterator();
 }

 public abstract FloatIterator iterator();



 /** Delegates to the corresponding type-specific method. */
 public boolean add( final Float o ) {
  return add( o.floatValue() );
 }

 /** Delegates to the corresponding type-specific method. */
 public boolean rem( final Object o ) {
  return rem( ((((Float)(o)).floatValue())) );
 }

 /** Delegates to the corresponding type-specific method. */
 public boolean contains( final Object o ) {
  return contains( ((((Float)(o)).floatValue())) );
 }


 /** Checks whether this collection contains all elements from the given collection.
	 *
	 * @param c a collection.
	 * @return <code>true</code> if this collection contains all elements of the argument.
	 */

 public boolean containsAll( Collection<?> c ) {
  int n = c.size();

  final Iterator<?> i = c.iterator();
  while( n-- != 0 ) if ( ! contains( i.next() ) ) return false;

  return true;
 }


 /** Retains in this collection only elements from the given collection.
	 *
	 * @param c a collection.
	 * @return <code>true</code> if this collection changed as a result of the call.
	 */

 public boolean retainAll( Collection<?> c ) {
  boolean retVal = false;
  int n = size();

  final Iterator<?> i = iterator();
  while( n-- != 0 ) {
   if ( ! c.contains( i.next() ) ) {
    i.remove();
    retVal = true;
   }
  }

  return retVal;
 }

 /** Remove from this collection all elements in the given collection.
	 * If the collection is an instance of this class, it uses faster iterators.
	 *
	 * @param c a collection.
	 * @return <code>true</code> if this collection changed as a result of the call.
	 */

 public boolean removeAll( Collection<?> c ) {
  boolean retVal = false;
  int n = c.size();

  final Iterator<?> i = c.iterator();
  while( n-- != 0 ) if ( remove( i.next() ) ) retVal = true;

  return retVal;
 }

 public boolean isEmpty() {
  return size() == 0;
 }

 public String toString() {
  final StringBuilder s = new StringBuilder();
  final FloatIterator i = iterator();
  int n = size();
  float k;
  boolean first = true;

  s.append("{");

  while(n-- != 0) {
   if (first) first = false;
   else s.append(", ");
   k = i.nextFloat();



    s.append(String.valueOf(k));
  }

  s.append("}");
  return s.toString();
 }

}
