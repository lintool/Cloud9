

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
import java.util.List;
/** A type-specific {@link List}; provides some additional methods that use polymorphism to avoid (un)boxing. 
 *
 * <P>Note that this type-specific interface extends {@link Comparable}: it is expected that implementing
 * classes perform a lexicographical comparison using the standard operator "less then" for primitive types,
 * and the usual {@link Comparable#compareTo(Object) compareTo()} method for objects.
 *
 * <P>Additionally, this interface strengthens {@link #listIterator()},
 * {@link #listIterator(int)} and {@link #subList(int,int)}.
 *
 * <P>Besides polymorphic methods, this interfaces specifies methods to copy into an array or remove contiguous
 * sublists. Although the abstract implementation of this interface provides simple, one-by-one implementations
 * of these methods, it is expected that concrete implementation override them with optimized versions.
 *
 * @see List
 */
public interface LongList extends List<Long>, Comparable<List<? extends Long>>, LongCollection {
 /** Returns a type-specific iterator on the elements of this list (in proper sequence).
	 *
	 * Note that this specification strengthens the one given in {@link List#iterator()}.
	 * It would not be normally necessary, but {@link java.lang.Iterable#iterator()} is bizarrily re-specified
	 * in {@link List}.
	 *
	 * @return an iterator on the elements of this list (in proper sequence).
	 */
 LongIterator iterator();
 /** Returns a type-specific list iterator on the list.
	 *
	 * @see #listIterator()
	 * @deprecated As of <code>fastutil</code> 5, replaced by {@link #listIterator()}.
	 */
 @Deprecated
 LongListIterator longListIterator();
 /** Returns a type-specific list iterator on the list starting at a given index.
	 *
	 * @see #listIterator(int)
	 * @deprecated As of <code>fastutil</code> 5, replaced by {@link #listIterator(int)}.
	 */
 @Deprecated
 LongListIterator longListIterator( int index );

 /** Returns a type-specific list iterator on the list.
	 *
	 * @see #listIterator()
	 */
 LongListIterator listIterator();

 /** Returns a type-specific list iterator on the list starting at a given index.
	 *
	 * @see #listIterator(int)
	 */
 LongListIterator listIterator( int index );

 /** Returns a type-specific view of the portion of this list from the index <code>from</code>, inclusive, to the index <code>to</code>, exclusive.
	 * @see List#subList(int,int)
	 * @deprecated As of <code>fastutil</code> 5, replaced by {@link #subList(int,int)}.
	 */
 @Deprecated
 LongList longSubList( int from, int to );

 /** Returns a type-specific view of the portion of this list from the index <code>from</code>, inclusive, to the index <code>to</code>, exclusive.
	 *
	 * <P>Note that this specification strengthens the one given in {@link List#subList(int,int)}.
	 *
	 * @see List#subList(int,int)
	 */
 LongList subList(int from, int to);


 /** Sets the size of this list.
	 *
	 * <P>If the specified size is smaller than the current size, the last elements are
	 * discarded. Otherwise, they are filled with 0/<code>null</code>/<code>false</code>.
	 *
	 * @param size the new size.
	 */

 void size( int size );

 /** Copies (hopefully quickly) elements of this type-specific list into the given array.
	 *
	 * @param from the start index (inclusive).
	 * @param a the destination array.
	 * @param offset the offset into the destination array where to store the first element copied.
	 * @param length the number of elements to be copied.
	 */
 void getElements( int from, long a[], int offset, int length );

 /** Removes (hopefully quickly) elements of this type-specific list.
	 *
	 * @param from the start index (inclusive).
	 * @param to the end index (exclusive).
	 */
 void removeElements( int from, int to );

 /** Add (hopefully quickly) elements to this type-specific list.
	 *
	 * @param index the index at which to add elements.
	 * @param a the array containing the elements.
	 */
 void addElements( int index, long a[] );

 /** Add (hopefully quickly) elements to this type-specific list.
	 *
	 * @param index the index at which to add elements.
	 * @param a the array containing the elements.
	 * @param offset the offset of the first element to add.
	 * @param length the number of elements to add.
	 */
 void addElements( int index, long a[], int offset, int length );



 /**
	 * @see List#add(int,Object)
	 */
 void add( int index, long key );

 /**
	 * @see List#add(int,Object)
	 */
 boolean addAll( int index, LongCollection c );

 /**
	 * @see List#add(int,Object)
	 */
 boolean addAll( int index, LongList c );

 /**
	 * @see List#add(int,Object)
	 */
 boolean addAll( LongList c );

 /**
	 * @see List#get(int)
	 */
 long getLong( int index );

 /**
	 * @see List#indexOf(Object)
	 */
 int indexOf( long k );

 /**
	 * @see List#lastIndexOf(Object)
	 */
 int lastIndexOf( long k );

 /**
	 * @see List#remove(int)
	 */
 long removeLong( int index );

 /**
	 * @see List#set(int,Object)
	 */
 long set( int index, long k );




}
