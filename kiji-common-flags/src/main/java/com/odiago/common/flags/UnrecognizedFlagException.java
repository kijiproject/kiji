// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

/**
 * When there are multiple flags declared with the same name.
 */
public class UnrecognizedFlagException extends RuntimeException {
  public UnrecognizedFlagException(String flagName) {
    super("Flag " + flagName + " was never declared");
  }
}
