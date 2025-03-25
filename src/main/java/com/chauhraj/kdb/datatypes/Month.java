package com.chauhraj.kdb.datatypes;

/**
 * Represents kdb+ month type, which is the number of months since Jan 2000.
 */
public class Month implements Comparable<Month> {
  /** Number of months since Jan 2000 */
  public int i;

  /**
   * Create a KDB+ representation of 'month' type from the q language
   * (a month value is the count of months since the beginning of the millennium.
   * Post-milieu is positive and pre is negative)
   * @param x Number of months from millennium
   */
  public Month(int x) {
    i = x;
  }

  @Override
  public String toString() {
    int m = i + 24000;
    int y = m / 12;
    return i == Integer.MIN_VALUE ? "" : i2(y / 100) + i2(y % 100) + "-" + i2(1 + m % 12);
  }

  @Override
  public boolean equals(final Object o) {
    return ((o instanceof Month) && (((Month)o).i == i));
  }

  @Override
  public int hashCode() {
    return i;
  }

  @Override
  public int compareTo(Month m) {
    return i - m.i;
  }

  private static String i2(int i) {
    return new java.text.DecimalFormat("00").format(i);
  }
} 