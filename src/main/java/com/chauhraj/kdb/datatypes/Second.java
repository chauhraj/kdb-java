package com.chauhraj.kdb.datatypes;

/**
 * Represents kdb+ second type, which is a point in time represented in seconds since midnight.
 */
public class Second implements Comparable<Second> {
  /** Number of seconds since midnight. */
  public int i;

  /**
   * Create a KDB+ representation of 'second' type from the q language
   * (point in time represented in seconds since midnight)
   * @param x Number of seconds since midnight
   */
  public Second(int x) {
    i = x;
  }

  @Override
  public String toString() {
    return i == Integer.MIN_VALUE ? "" : new Minute(i / 60).toString() + ':' + i2(i % 60);
  }

  @Override
  public boolean equals(final Object o) {
    return ((o instanceof Second) && (((Second)o).i == i));
  }

  @Override
  public int hashCode() {
    return i;
  }

  @Override
  public int compareTo(Second s) {
    return i - s.i;
  }

  private static String i2(int i) {
    return new java.text.DecimalFormat("00").format(i);
  }
} 