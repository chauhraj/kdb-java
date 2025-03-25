package com.chauhraj.kdb.datatypes;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Represents kdb+ timestamp type, which is a point in time represented in nanoseconds since midnight.
 */
public class Timespan implements Comparable<Timespan> {
  /** Number of nanoseconds since midnight. */
  public long j;

  /**
   * Create a KDB+ representation of 'timespan' type from the q language
   * (point in time represented in nanoseconds since midnight)
   * @param x Number of nanoseconds since midnight
   */
  public Timespan(long x) {
    j = x;
  }

  /** Constructs {@code Timespan} using current time since midnight and default timezone. */
  public Timespan() {
    this(TimeZone.getDefault());
  }

  /**
   * Constructs {@code Timespan} using current time since midnight and default timezone.
   * @param tz {@code TimeZone} to use for deriving midnight.
   */
  public Timespan(TimeZone tz) {
    Calendar c = Calendar.getInstance(tz);
    long now = c.getTimeInMillis();
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    j = (now - c.getTimeInMillis()) * 1000000L;
  }

  @Override
  public String toString() {
    if (j == Long.MIN_VALUE)
      return "";
    String s = j < 0 ? "-" : "";
    long jj = j < 0 ? -j : j;
    int d = ((int)(jj / 86400000000000L));
    if (d != 0)
      s += d + "D";
    return s + i2((int)((jj % 86400000000000L) / 3600000000000L)) + ":" +
           i2((int)((jj % 3600000000000L) / 60000000000L)) + ":" +
           i2((int)((jj % 60000000000L) / 1000000000L)) + "." +
           i9((int)(jj % 1000000000L));
  }

  @Override
  public int compareTo(Timespan t) {
    if (j > t.j)
      return 1;
    return j < t.j ? -1 : 0;
  }

  @Override
  public boolean equals(final Object o) {
    return ((o instanceof Timespan) && (((Timespan)o).j == j));
  }

  @Override
  public int hashCode() {
    return (int)(j ^ (j >>> 32));
  }

  private static String i2(int i) {
    return new java.text.DecimalFormat("00").format(i);
  }

  private static String i9(int i) {
    return new java.text.DecimalFormat("000000000").format(i);
  }
} 