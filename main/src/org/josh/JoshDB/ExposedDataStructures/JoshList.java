package org.josh.JoshDB.ExposedDataStructures;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class JoshList<T> implements List<T>
{
  public JoshList()
  {

  }

  @Override
  public int size()
  {
    return 0;
  }

  @Override
  public boolean isEmpty()
  {
    return false;
  }

  @Override
  public boolean contains(Object o)
  {
    if (o == null || !o.getClass().isAssignableFrom(this.getClass()))
    {
      return false;
    }

    return false;
  }

  @Override
  public Iterator<T> iterator()
  {
    return null;
  }

  @Override
  public void forEach(Consumer<? super T> action)
  {

  }

  @Override
  public Object[] toArray()
  {
    return new Object[0];
  }

  @Override
  public <T1> T1[] toArray(T1[] a)
  {
    return null;
  }

  @Override
  public boolean add(T t)
  {
    return false;
  }

  @Override
  public boolean remove(Object o)
  {
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> c)
  {
    return false;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c)
  {
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    return false;
  }

  @Override
  public boolean removeIf(Predicate<? super T> filter)
  {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    return false;
  }

  @Override
  public void replaceAll(UnaryOperator<T> operator)
  {

  }

  @Override
  public void sort(Comparator<? super T> c)
  {

  }

  @Override
  public void clear()
  {

  }

  @Override
  public boolean equals(Object o)
  {
    return false;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public T get(int index)
  {
    return null;
  }

  @Override
  public T set(int index, T element)
  {
    return null;
  }

  @Override
  public void add(int index, T element)
  {

  }

  @Override
  public T remove(int index)
  {
    return null;
  }

  @Override
  public int indexOf(Object o)
  {
    return 0;
  }

  @Override
  public int lastIndexOf(Object o)
  {
    return 0;
  }

  @Override
  public ListIterator<T> listIterator()
  {
    return null;
  }

  @Override
  public ListIterator<T> listIterator(int index)
  {
    return null;
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex)
  {
    return null;
  }

  @Override
  public Spliterator<T> spliterator()
  {
    return null;
  }

  @Override
  public Stream<T> stream()
  {
    return null;
  }

  @Override
  public Stream<T> parallelStream()
  {
    return null;
  }
}
