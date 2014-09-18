// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

/**
 * When there are multiple flags declared with the same name.
 */
public class UnsupportedFlagTypeException extends RuntimeException {
  public UnsupportedFlagTypeException(String fieldName) {
    super("The @Flag annotation does not support the type for field " + fieldName);
  }
}
