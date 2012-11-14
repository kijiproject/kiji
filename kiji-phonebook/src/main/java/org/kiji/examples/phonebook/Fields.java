/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.examples.phonebook;

/**
 * Constants that define column families and qualifiers used in this application.
 */
public final class Fields {
  /** Private c'tor denies instantiation. */
  private Fields() {
  }

  /**
   * Constants defining the column family name and column qualifiers for a user's information
   * follows.
   */
  public static final String INFO_FAMILY = "info";

  /** Qualifier holding user's first name. */
  public static final String FIRST_NAME = "firstname";

  /** Qualifier holding user's last name. */
  public static final String LAST_NAME = "lastname";

  /** Qualifier holding user's email address. */
  public static final String EMAIL = "email";

  /** Qualifier holding user's telephone number. */
  public static final String TELEPHONE = "telephone";

  /** Qualifier holding user's mailing address. */
  public static final String ADDRESS = "address";

  /**
   * Constants defining the column family name and column qualifiers for information derived
   * from a user's address.
   */
  public static final String DERIVED_FAMILY = "derived";

  /** Qualifier holding the first address line. */
  public static final String ADDR_LINE_1 = "addr1";

  /** Qualifier holding the apartment number of an address. */
  public static final String APT_NUMBER = "apt";

  /** Qualifier holding the second address line. */
  public static final String ADDR_LINE_2 = "addr2";

  /** Qualifier holding the address city. */
  public static final String CITY = "city";

  /** Qualifier holding the address state. */
  public static final String STATE = "state";

  /** Qualifier holding the address zip code. */
  public static final String ZIP = "zip";
}
