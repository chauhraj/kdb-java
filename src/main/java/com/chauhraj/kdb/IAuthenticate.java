package com.chauhraj.kdb;

/**
 * Interface for authenticating incoming connections based on the KDB+ handshake.
 */
public interface IAuthenticate {
  /**
   * Checks authentication string provided to allow/reject connection.
   * @param s String containing username:password for authentication
   * @return true if credentials accepted.
   */
  boolean authenticate(String s);
} 