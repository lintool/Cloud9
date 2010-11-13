

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
import java.util.List;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Collection;
import java.util.NoSuchElementException;
/**  An abstract class providing basic methods for lists implementing a type-specific list interface.
 *
 * <P>As an additional bonus, this class implements on top of the list operations a type-specific stack.
 */
public abstract class AbstractFloatList extends AbstractFloatCollection implements FloatList , FloatStack {
 protected AbstractFloatList() {}
 /** Ensures that the given index is nonnegative and not greater than the list size.
	 *
	 * @param index an index.
	 * @throws IndexOutOfBoundsException if the given index is negative or greater than the list size.
	 */
 protected void ensureIndex( final int index ) {
  if ( index < 0 ) throw new IndexOutOfBoundsException( "Index (" + index + ") is negative" );
  if ( index > size() ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than list size (" + ( size() ) + ")" );
 }
 /** Ensures that the given index is nonnegative and smaller than the list size.
	 *
	 * @param index an index.
	 * @throws IndexOutOfBoundsException if the given index is negative or not smaller than the list size.
	 */
 protected void ensureRestrictedIndex( final int index ) {
  if ( index < 0 ) throw new IndexOutOfBoundsException( "Index (" + index + ") is negative" );
  if ( index >= size() ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to list size (" + ( size() ) + ")" );
 }
 public void add( final int index, final float k ) {
  throw new UnsupportedOperationException();
 }
 public boolean add( final float k ) {
  add( size(), k );
  return true;
 }
 public float removeFloat( int i ) {
  throw new UnsupportedOperationException();
 }
 public float set( final int index, final float k ) {
  throw new UnsupportedOperationException();
 }
 public boolean addAll( int index, final Collection<? extends Float> c ) {
  ensureIndex( index );
  int n = c.size();
  if ( n == 0 ) return false;
  Iterator<? extends Float> i = c.iterator();
  while( n-- != 0 ) add( index++, i.next() );
  return true;
 }
 /** Delegates to a more generic method. */
 public boolean addAll( final Collection<? extends Float> c ) {
  return addAll( size(), c );
 }
 /** Delegates to the new covariantly stronger generic method. */
 @Deprecated
 public FloatListIterator floatListIterator() {
  return listIterator();
 }
 /** Delegates to the new covariantly stronger generic method. */
 @Deprecated
 public FloatListIterator floatListIterator( final int index ) {
  return listIterator( index );
 }
 public FloatIterator iterator() {
  return listIterator();
 }
 public FloatListIterator listIterator() {
  return listIterator( 0 );
 }
 public FloatListIterator listIterator( final int index ) {
  return new AbstractFloatListIterator () {
    int pos = index, last = -1;
    public boolean hasNext() { return pos < AbstractFloatList.this.size(); }
    public boolean hasPrevious() { return pos > 0; }
    public float nextFloat() { if ( ! hasNext() ) throw new NoSuchElementException(); return AbstractFloatList.this.getFloat( last = pos++ ); }
    public float previousFloat() { if ( ! hasPrevious() ) throw new NoSuchElementException(); return AbstractFloatList.this.getFloat( last = --pos ); }
    public int nextIndex() { return pos; }
    public int previousIndex() { return pos - 1; }
    public void add( float k ) {
     if ( last == -1 ) throw new IllegalStateException();
     AbstractFloatList.this.add( pos++, k );
     last = -1;
    }
    public void set( float k ) {
     if ( last == -1 ) throw new IllegalStateException();
     AbstractFloatList.this.set( last, k );
    }
    public void remove() {
     if ( last == -1 ) throw new IllegalStateException();
     AbstractFloatList.this.removeFloat( last );
     /* If the last operation was a next(), we are removing an element *before* us, and we must decrease pos correspondingly. */
     if ( last < pos ) pos--;
     last = -1;
    }
   };
 }


 public boolean contains( final float k ) {
  return indexOf( k ) >= 0;
 }

 public int indexOf( final float k ) {



  final FloatListIterator i = listIterator();
  float e;
  while( i.hasNext() ) {
   e = i.nextFloat();
   if ( ( (k) == (e) ) ) return i.previousIndex();
  }
  return -1;
 }

 public int lastIndexOf( final float k ) {



  FloatListIterator i = listIterator( size() );
  float e;
  while( i.hasPrevious() ) {
   e = i.previousFloat();
   if ( ( (k) == (e) ) ) return i.nextIndex();
  }
  return -1;
 }

 public void size( final int size ) {
  int i = size();
  if ( size > i ) while( i++ < size ) add( (0) );
  else while( i-- != size ) remove( i );
 }


 public FloatList subList( final int from, final int to ) {
  ensureIndex( from );
  ensureIndex( to );
  if ( from > to ) throw new IndexOutOfBoundsException( "Start index (" + from + ") is greater than end index (" + to + ")" );

  return new FloatSubList ( this, from, to );
 }

 /** Delegates to the new covariantly stronger generic method. */

 @Deprecated
 public FloatList floatSubList( final int from, final int to ) {
  return subList( from, to );
 }

 /** Removes elements of this type-specific list one-by-one. 
	 *
	 * <P>This is a trivial iterator-based implementation. It is expected that
	 * implementations will override this method with a more optimized version.
	 *
	 *
	 * @param from the start index (inclusive).
	 * @param to the end index (exclusive).
	 */

 public void removeElements( final int from, final int to ) {
  ensureIndex( to );
  FloatListIterator i = listIterator( from );
  int n = to - from;
  if ( n < 0 ) throw new IllegalArgumentException( "Start index (" + from + ") is greater than end index (" + to + ")" );
  while( n-- != 0 ) {
   i.nextFloat();
   i.remove();
  }
 }

 /** Adds elements to this type-specific list one-by-one. 
	 *
	 * <P>This is a trivial iterator-based implementation. It is expected that
	 * implementations will override this method with a more optimized version.
	 *
	 * @param index the index at which to add elements.
	 * @param a the array containing the elements.
	 * @param offset the offset of the first element to add.
	 * @param length the number of elements to add.
	 */

 public void addElements( int index, final float a[], int offset, int length ) {
  ensureIndex( index );
  if ( offset < 0 ) throw new ArrayIndexOutOfBoundsException( "Offset (" + offset + ") is negative" );
  if ( offset + length > a.length ) throw new ArrayIndexOutOfBoundsException( "End index (" + ( offset + length ) + ") is greater than array length (" + a.length + ")" );
  while( length-- != 0 ) add( index++, a[ offset++ ] );
 }

 public void addElements( final int index, final float a[] ) {
  addElements( index, a, 0, a.length );
 }

 /** Copies element of this type-specific list into the given array one-by-one.
	 *
	 * <P>This is a trivial iterator-based implementation. It is expected that
	 * implementations will override this method with a more optimized version.
	 *
	 * @param from the start index (inclusive).
	 * @param a the destination array.
	 * @param offset the offset into the destination array where to store the first element copied.
	 * @param length the number of elements to be copied.
	 */

 public void getElements( final int from, final float a[], int offset, int length ) {
  FloatListIterator i = listIterator( from );
  if ( offset < 0 ) throw new ArrayIndexOutOfBoundsException( "Offset (" + offset + ") is negative" );
  if ( offset + length > a.length ) throw new ArrayIndexOutOfBoundsException( "End index (" + ( offset + length ) + ") is greater than array length (" + a.length + ")" );
  if ( from + length > size() ) throw new IndexOutOfBoundsException( "End index (" + ( from + length ) + ") is greater than list size (" + size() + ")" );
  while( length-- != 0 ) a[ offset++ ] = i.nextFloat();
 }


 private boolean valEquals( final Object a, final Object b ) {
  return a == null ? b == null : a.equals( b );
 }


 public boolean equals( final Object o ) {
  if ( o == this ) return true;
  if ( ! ( o instanceof List ) ) return false;
  final List<?> l = (List<?>)o;
  int s = size();
  if ( s != l.size() ) return false;

  final ListIterator<?> i1 = listIterator(), i2 = l.listIterator();




  while( s-- != 0 ) if ( ! valEquals( i1.next(), i2.next() ) ) return false;

  return true;
 }


    /** Compares this list to another object. If the
     * argument is a {@link java.util.List}, this method performs a lexicographical comparison; otherwise,
     * it throws a <code>ClassCastException</code>.
     *
     * @param l an list.
     * @return if the argument is a {@link java.util.List}, a negative integer,
     * zero, or a positive integer as this list is lexicographically less than, equal
     * to, or greater than the argument.
     * @throws ClassCastException if the argument is not a list.
     */

 @SuppressWarnings("unchecked")
 public int compareTo( final List<? extends Float> l ) {
  if ( l == this ) return 0;

  if ( l instanceof FloatList ) {

   final FloatListIterator i1 = listIterator(), i2 = ((FloatList)l).listIterator();
   int r;
   float e1, e2;

   while( i1.hasNext() && i2.hasNext() ) {
    e1 = i1.nextFloat();
    e2 = i2.nextFloat();
    if ( ( r = ( (e1) < (e2) ? -1 : ( (e1) == (e2) ? 0 : 1 ) ) ) != 0 ) return r;
   }
   return i2.hasNext() ? -1 : ( i1.hasNext() ? 1 : 0 );
  }

  ListIterator<? extends Float> i1 = listIterator(), i2 = l.listIterator();
  int r;

  while( i1.hasNext() && i2.hasNext() ) {
   if ( ( r = ((Comparable<? super Float>)i1.next()).compareTo( i2.next() ) ) != 0 ) return r;
  }
  return i2.hasNext() ? -1 : ( i1.hasNext() ? 1 : 0 );
 }


 /** Returns the hash code for this list, which is identical to {@link java.util.List#hashCode()}.
	 *
	 * @return the hash code for this list.
	 */
 public int hashCode() {
  FloatIterator i = iterator();
  int h = 1, s = size();
  while ( s-- != 0 ) {
   float k = i.nextFloat();
   h = 31 * h + it.unimi.dsi.fastutil.HashCommon.float2int(k);
  }
  return h;
 }


 public void push( float o ) {
  add( o );
 }

 public float popFloat() {
  if ( isEmpty() ) throw new NoSuchElementException();
  return removeFloat( size() - 1 );
 }

 public float topFloat() {
  if ( isEmpty() ) throw new NoSuchElementException();
  return getFloat( size() - 1 );
 }

 public float peekFloat( int i ) {
  return getFloat( size() - 1 - i );
 }



 public boolean rem( float k ) {
  int index = indexOf( k );
  if ( index == -1 ) return false;
  removeFloat( index );
  return true;
 }

 /** Delegates to <code>rem()</code>. */
 public boolean remove( final Object o ) {
  return rem( ((((Float)(o)).floatValue())) );
 }

 /** Delegates to a more generic method. */
 public boolean addAll( final int index, final FloatCollection c ) {
  return addAll( index, (Collection<? extends Float>)c );
 }

 /** Delegates to a more generic method. */
 public boolean addAll( final int index, final FloatList l ) {
  return addAll( index, (FloatCollection)l );
 }

 public boolean addAll( final FloatCollection c ) {
  return addAll( size(), c );
 }

 public boolean addAll( final FloatList l ) {
  return addAll( size(), l );
 }

 /** Delegates to the corresponding type-specific method. */
 public void add( final int index, final Float ok ) {
  add( index, ok.floatValue() );
 }

 /** Delegates to the corresponding type-specific method. */
 public Float set( final int index, final Float ok ) {
  return (Float.valueOf(set( index, ok.floatValue() )));
 }

 /** Delegates to the corresponding type-specific method. */
 public Float get( final int index ) {
  return (Float.valueOf(getFloat( index )));
 }

 /** Delegates to the corresponding type-specific method. */
 public int indexOf( final Object ok) {
  return indexOf( ((((Float)(ok)).floatValue())) );
 }

 /** Delegates to the corresponding type-specific method. */
 public int lastIndexOf( final Object ok ) {
  return lastIndexOf( ((((Float)(ok)).floatValue())) );
 }

 /** Delegates to the corresponding type-specific method. */
 public Float remove( final int index ) {
  return (Float.valueOf(removeFloat( index )));
 }

 /** Delegates to the corresponding type-specific method. */
 public void push( Float o ) {
  push( o.floatValue() );
 }

 /** Delegates to the corresponding type-specific method. */
 public Float pop() {
  return Float.valueOf( popFloat() );
 }

 /** Delegates to the corresponding type-specific method. */
 public Float top() {
  return Float.valueOf( topFloat() );
 }

 /** Delegates to the corresponding type-specific method. */
 public Float peek( int i ) {
  return Float.valueOf( peekFloat( i ) );
 }




 public String toString() {
  final StringBuilder s = new StringBuilder();
  final FloatIterator i = iterator();
  int n = size();
  float k;
  boolean first = true;

  s.append("[");

  while( n-- != 0 ) {
   if (first) first = false;
   else s.append(", ");
   k = i.nextFloat();



    s.append( String.valueOf( k ) );
  }

  s.append("]");
  return s.toString();
 }


 public static class FloatSubList extends AbstractFloatList implements java.io.Serializable {
     public static final long serialVersionUID = -7046029254386353129L;
  /** The list this sublist restricts. */
  protected final FloatList l;
  /** Initial (inclusive) index of this sublist. */
  protected final int from;
  /** Final (exclusive) index of this sublist. */
  protected int to;

  private static final boolean ASSERTS = false;

  public FloatSubList( final FloatList l, final int from, final int to ) {
   this.l = l;
   this.from = from;
   this.to = to;
  }

  private void assertRange() {
   if ( ASSERTS ) {
    assert from <= l.size();
    assert to <= l.size();
    assert to >= from;
   }
  }

  public boolean add( final float k ) {
   l.add( to, k );
   to++;
   if ( ASSERTS ) assertRange();
   return true;
  }

  public void add( final int index, final float k ) {
   ensureIndex( index );
   l.add( from + index, k );
   to++;
   if ( ASSERTS ) assertRange();
  }

  public boolean addAll( final int index, final Collection<? extends Float> c ) {
   ensureIndex( index );
   to += c.size();
   if ( ASSERTS ) {
    boolean retVal = l.addAll( from + index, c );
    assertRange();
    return retVal;
   }
   return l.addAll( from + index, c );
  }

  public float getFloat( int index ) {
   ensureRestrictedIndex( index );
   return l.getFloat( from + index );
  }

  public float removeFloat( int index ) {
   ensureRestrictedIndex( index );
   to--;
   return l.removeFloat( from + index );
  }

  public float set( int index, float k ) {
   ensureRestrictedIndex( index );
   return l.set( from + index, k );
  }

  public void clear() {
   removeElements( 0, size() );
   if ( ASSERTS ) assertRange();
  }

  public int size() {
   return to - from;
  }

  public void getElements( final int from, final float[] a, final int offset, final int length ) {
   ensureIndex( from );
   if ( from + length > size() ) throw new IndexOutOfBoundsException( "End index (" + from + length + ") is greater than list size (" + size() + ")" );
   l.getElements( this.from + from, a, offset, length );
  }

  public void removeElements( final int from, final int to ) {
   ensureIndex( from );
   ensureIndex( to );
   l.removeElements( this.from + from, this.from + to );
   this.to -= ( to - from );
   if ( ASSERTS ) assertRange();
  }

  public void addElements( int index, final float a[], int offset, int length ) {
   ensureIndex( index );
   l.addElements( this.from + index, a, offset, length );
   this.to += length;
   if ( ASSERTS ) assertRange();
  }

  public FloatListIterator listIterator( final int index ) {
   ensureIndex( index );

   return new AbstractFloatListIterator () {
     int pos = index, last = -1;

     public boolean hasNext() { return pos < size(); }
     public boolean hasPrevious() { return pos > 0; }
     public float nextFloat() { if ( ! hasNext() ) throw new NoSuchElementException(); return l.getFloat( from + ( last = pos++ ) ); }
     public float previousFloat() { if ( ! hasPrevious() ) throw new NoSuchElementException(); return l.getFloat( from + ( last = --pos ) ); }
     public int nextIndex() { return pos; }
     public int previousIndex() { return pos - 1; }
     public void add( float k ) {
      if ( last == -1 ) throw new IllegalStateException();
      FloatSubList.this.add( pos++, k );
      last = -1;
      if ( ASSERTS ) assertRange();
     }
     public void set( float k ) {
      if ( last == -1 ) throw new IllegalStateException();
      FloatSubList.this.set( last, k );
     }
     public void remove() {
      if ( last == -1 ) throw new IllegalStateException();
      FloatSubList.this.removeFloat( last );
      /* If the last operation was a next(), we are removing an element *before* us, and we must decrease pos correspondingly. */
      if ( last < pos ) pos--;
      last = -1;
      if ( ASSERTS ) assertRange();
     }
    };
  }

  public FloatList subList( final int from, final int to ) {
   ensureIndex( from );
   ensureIndex( to );
   if ( from > to ) throw new IllegalArgumentException( "Start index (" + from + ") is greater than end index (" + to + ")" );

   return new FloatSubList ( this, from, to );
  }



  public boolean rem( float k ) {
   int index = indexOf( k );
   if ( index == -1 ) return false;
   to--;
   l.removeFloat( from + index );
   if ( ASSERTS ) assertRange();
   return true;
  }

  public boolean remove( final Object o ) {
   return rem( ((((Float)(o)).floatValue())) );
  }

  public boolean addAll( final int index, final FloatCollection c ) {
   ensureIndex( index );
   to += c.size();
   if ( ASSERTS ) {
    boolean retVal = l.addAll( from + index, c );
    assertRange();
    return retVal;
   }
   return l.addAll( from + index, c );
  }

  public boolean addAll( final int index, final FloatList l ) {
   ensureIndex( index );
   to += l.size();
   if ( ASSERTS ) {
    boolean retVal = this.l.addAll( from + index, l );
    assertRange();
    return retVal;
   }
   return this.l.addAll( from + index, l );
  }
 }
}
