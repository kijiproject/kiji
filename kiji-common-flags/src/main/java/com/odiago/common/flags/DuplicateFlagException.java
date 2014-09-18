// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

/**
 * When there are multiple flags declared with the same name.
 */
public class DuplicateFlagException extends RuntimeException {
  public DuplicateFlagException(String flagName) {
    super("Duplicate @Flag declaration: " + flagName);
  }
}
