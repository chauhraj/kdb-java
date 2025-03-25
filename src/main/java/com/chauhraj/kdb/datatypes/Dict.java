package com.chauhraj.kdb.datatypes;

/**
 * Represents the kdb+ dictionary type, which is a mapping from a key list to a value list.
 * The two lists must have the same count.
 * An introduction can be found at <a href="https://code.kx.com/q4m3/5_Dictionaries/">https://code.kx.com/q4m3/5_Dictionaries/</a>
 */
public class Dict {
  /** Dict keys */
  public Object x;
  /** Dict values */
  public Object y;

  /**
   * Create a representation of the KDB+ dictionary type, which is a
   * mapping between keys and values
   * @param keys Keys to store. Should be an array type when using multiple values.
   * @param vals Values to store. Index of each value should match the corresponding associated key.
   *  Should be an array type when using multiple values.
   */
  public Dict(Object keys, Object vals) {
    x = keys;
    y = vals;
  }

  @Override
  public String toString() {
    return "Dict{" + "x=" + x + ", y=" + y + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dict dict = (Dict) o;
    return x.equals(dict.x) && y.equals(dict.y);
  }

  @Override
  public int hashCode() {
    int result = x.hashCode();
    result = 31 * result + y.hashCode();
    return result;
  }
} 