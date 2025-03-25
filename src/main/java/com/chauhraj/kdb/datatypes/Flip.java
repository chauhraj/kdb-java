package com.chauhraj.kdb.datatypes;

/**
 * Represents a kdb+ table (an array of column names, and an array of arrays containing the column data).
 * q tables are column-oriented, in contrast to the row-oriented tables in relational databases.
 * An introduction can be found at <a href="https://code.kx.com/q4m3/8_Tables/">https://code.kx.com/q4m3/8_Tables/</a>
 */
public class Flip {
  /** Array of column names. */
  public String[] x;
  /** Array of arrays of the column values. */
  public Object[] y;

  /**
   * Create a Flip (KDB+ table) from the values stored in a Dict.
   * @param dict Values stored in the dict should be an array of Strings for the column names (keys), with an
   * array of arrays for the column values
   */
  public Flip(Dict dict) {
    x = (String[]) dict.x;
    y = (Object[]) dict.y;
  }

  /**
   * Create a Flip (KDB+ table) from array of column names and array of arrays of the column values.
   * @param x Array of column names
   * @param y Array of arrays of the column values
   */
  public Flip(String[] x, Object[] y) {
    this.x = x;
    this.y = y;
  }

  /**
   * Returns the column values given the column name
   * @param s The column name
   * @return The value(s) associated with the column name which can be casted to an array of objects.
   */
  public Object at(String s) {
    return y[find(x, s)];
  }

  private static int find(String[] x, String y) {
    int i = 0;
    while (i < x.length && !x[i].equals(y))
      ++i;
    return i;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Flip{");
    sb.append("x=").append(java.util.Arrays.toString(x));
    sb.append(", y=").append(java.util.Arrays.deepToString(y));
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Flip flip = (Flip) o;
    return java.util.Arrays.equals(x, flip.x) && java.util.Arrays.deepEquals(y, flip.y);
  }

  @Override
  public int hashCode() {
    int result = java.util.Arrays.hashCode(x);
    result = 31 * result + java.util.Arrays.deepHashCode(y);
    return result;
  }
} 