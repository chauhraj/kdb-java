package com.chauhraj.kdb.datatypes;

/**
 * Represents kdb+ minute type, which is a time represented as the number of minutes from midnight.
 */
public class Minute implements Comparable<Minute> {
  /** Number of minutes since midnight. */
  public int i;

  /**
   * Create a KDB+ representation of 'minute' type from the q language
   * (point in time represented in minutes since midnight)
   * @param x Number of minutes since midnight
   */
  public Minute(int x) {
    i = x;
  }

  @Override
  public String toString() {
    return i == Integer.MIN_VALUE ? "" : i2(i / 60) + ":" + i2(i % 60);
  }

  @Override
  public boolean equals(final Object o) {
    return ((o instanceof Minute) && (((Minute)o).i == i));
  }

  @Override
  public int hashCode() {
    return i;
  }

  @Override
  public int compareTo(Minute m) {
    return i - m.i;
  }

  private static String i2(int i) {
    return new java.text.DecimalFormat("00").format(i);
  }
} 