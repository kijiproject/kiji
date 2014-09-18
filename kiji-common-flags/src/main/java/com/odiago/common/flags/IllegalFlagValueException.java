// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

/**
 * When there are multiple flags declared with the same name.
 */
public class IllegalFlagValueException extends RuntimeException {
  public IllegalFlagValueException(String flagName, String flagValue) {
    super("Illegal value for flag " + flagName + ": " + flagValue);
  }
}
