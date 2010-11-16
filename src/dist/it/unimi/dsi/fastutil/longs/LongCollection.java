

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
import java.util.Collection;
/** A type-specific {@link Collection}; provides some additional methods
 * that use polymorphism to avoid (un)boxing.
 *
 * <P>Additionally, this class defines strengthens (again) {@link #iterator()} and defines
 * a slightly different semantics for {@link #toArray(Object[])}.
 *
 * @see Collection
 */
public interface LongCollection extends Collection<Long>, LongIterable {
 /** Returns a type-specific iterator on the elements of this collection.
	 *
	 * <p>Note that this specification strengthens the one given in 
	 * {@link java.lang.Iterable#iterator()}, which was already 
	 * strengthened in the corresponding type-specific class,
	 * but was weakened by the fact that this interface extends {@link Collection}.
	 *
	 * @return a type-specific iterator on the elements of this collection.
	 */
 LongIterator iterator();
 /** Returns a type-specific iterator on this elements of this collection.
	 *
	 * @see #iterator()
	 * @deprecated As of <code>fastutil</code> 5, replaced by {@link #iterator()}.
	 */
 @Deprecated
 LongIterator longIterator();
 /** Returns an containing the items of this collection;
	 * the runtime type of the returned array is that of the specified array. 
	 *
	 * <p><strong>Warning</strong>: Note that, contrarily to {@link Collection#toArray(Object[])}, this
	 * methods just writes all elements of this collection: no special 
	 * value will be added after the last one.
	 *
	 * @param a if this array is big enough, it will be used to store this collection.
	 * @return a primitive type array containing the items of this collection.
	 * @see Collection#toArray(Object[])
	 */
 <T> T[] toArray(T[] a);
 /**
	 * @see Collection#contains(Object)
	 */
 boolean contains( long key );
 /** Returns a primitive type array containing the items of this collection. 
	 * @return a primitive type array containing the items of this collection.
	 * @see Collection#toArray()
	 */
 long[] toLongArray();
 /** Returns a primitive type array containing the items of this collection.
	 *
	 * <p>Note that, contrarily to {@link Collection#toArray(Object[])}, this
	 * methods just writes all elements of this collection: no special 
	 * value will be added after the last one.
	 *
	 * @param a if this array is big enough, it will be used to store this collection.
	 * @return a primitive type array containing the items of this collection.
	 * @see Collection#toArray(Object[])
	 */
 long[] toLongArray( long a[] );
 /** Returns a primitive type array containing the items of this collection. 
	 *
	 * <p>Note that, contrarily to {@link Collection#toArray(Object[])}, this
	 * methods just writes all elements of this collection: no special 
	 * value will be added after the last one.
	 *
	 * @param a if this array is big enough, it will be used to store this collection.
	 * @return a primitive type array containing the items of this collection.
	 * @see Collection#toArray(Object[])
	 */
 long[] toArray( long a[] );
 /**
	 * @see Collection#add(Object)
	 */
 boolean add( long key );
 /** Note that this method should be called {@link java.util.Collection#remove(Object) remove()}, but the clash
	 * with the similarly named index-based method in the {@link java.util.List} interface
	 * forces us to use a distinguished name. For simplicity, the set interfaces reinstates
	 * <code>remove()</code>.
	 *
	 * @see Collection#remove(Object)
	 */
 boolean rem( long key );
 /**
	 * @see Collection#addAll(Collection)
	 */
 boolean addAll( LongCollection c );
 /**
	 * @see Collection#containsAll(Collection)
	 */
 boolean containsAll( LongCollection c );
 /**
	 * @see Collection#removeAll(Collection)
	 */
 boolean removeAll( LongCollection c );
 /**
	 * @see Collection#retainAll(Collection)
	 */
 boolean retainAll( LongCollection c );
}
