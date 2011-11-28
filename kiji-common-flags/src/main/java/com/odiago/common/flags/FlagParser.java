/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.odiago.common.flags;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides a static method init(), used to parse flags from a command line.
 */
public class FlagParser {

  /** Should not be constructed. */
  private FlagParser() {}

  /**
   * Pulls out all the fields that have been annotated with {@code Flag} attributes.
   *
   * @param obj The object containing the flag definitions.
   *
   * @return A map from flag name to its definition.
   */
  private static Map<String, FlagSpec> extractFlagDeclarations(Object obj) {
    Map<String, FlagSpec> flags = new TreeMap<String, FlagSpec>();
    Class<?> clazz = obj.getClass();
    do {
      for (Field field : clazz.getDeclaredFields()) {
        Flag flag = field.getAnnotation(Flag.class);
        if (flag != null) {
          FlagSpec flagSpec = new FlagSpec(field, flag, obj);
          String flagName = flagSpec.getName();
          if (flags.containsKey(flagName)) {
            throw new DuplicateFlagException(flagName);
          }
          flags.put(flagSpec.getName(), flagSpec);
        }
      }
    } while (null != (clazz = clazz.getSuperclass()));
    return flags;
  }

  /**
   * Parse the flags out of the command line arguments.  The non flag args are put into
   * nonFlagArgs.
   *
   * @param args The arguments to parse.
   * @param nonFlagArgs The remaining non-flag arguments.
   *
   * @return A map from flag-name to flag-value.
   */
  private static Map<String, String> parseArgs(String[] args, List<String> nonFlagArgs) {
    Map<String, String> parsedFlags = new TreeMap<String, String>();
    boolean ignoreTheRest = false;
    for (String arg : args) {
      if (arg.startsWith("-") && !ignoreTheRest) {
        if (arg.equals("--")) {
          // Ignore any flag-like args after this special symbol.
          ignoreTheRest = true;
          break;
        }
        String keyVal = arg.startsWith("--") ? arg.substring(2) : arg.substring(1);
        String[] splitKeyVal = keyVal.split("=", 2);
        String key = splitKeyVal[0];
        String value = splitKeyVal.length == 2 ? splitKeyVal[1] : "";
        parsedFlags.put(key, value);
      } else {
        nonFlagArgs.add(arg);
      }
    }
    return parsedFlags;
  }

  /**
   * Prints human-readable usage information to an output stream.
   *
   * @param flags The flag definitions.
   * @param out An output stream.
   */
  private static void printUsage(Map<String, FlagSpec> flags, PrintStream out) {
    final String FORMAT_STRING = "  --%s=<%s>\n%s\t(Default=%s)\n\n";
    if (!flags.containsKey("help")) {
      out.printf(FORMAT_STRING, "help", "boolean", "\tDisplay this help message\n", "false");
    }
    for (FlagSpec flag : flags.values()) {
      if (flag.isHidden()) {
        // Don't display usage for hidden flags.
        continue;
      }
      String usage = flag.getUsage();
      if (!usage.isEmpty()) {
        usage = "\t" + usage + "\n";
      }
      out.printf(FORMAT_STRING, flag.getName(), flag.getTypeName(), usage, flag.getDefaultValue());
    }
  }

  /**
   * Print the list of flags and their usage strings.
   *
   * @param obj The object containing the flag declarations.
   * @param out An output stream.
   */
  public static void printUsage(Object obj, PrintStream out) {
    printUsage(extractFlagDeclarations(obj), out);
  }

  /**
   * Parses the flags defined in obj out of args.  Prints usage and returns null
   * if the flags could not be parsed.  Otherwise it assigns the flag values to
   * the fields with @Flag declarations and returns the non-flag arguments.
   *
   * @param obj The instance of the class containing flag declarations.
   * @param args The command-line arguments.
   * @param out Where to print usage info if there is a parsing error.
   *
   * @return The non-flag arguments, or null if the flags were not parsed.
   *
   * @throws DuplicateFlagException If there are duplicate flags.
   */
  public static List<String> init(Object obj, String[] args, PrintStream out) {
    List<String> nonFlagArgs = new ArrayList<String>();
    Map<String, String> parsedFlags = parseArgs(args, nonFlagArgs);
    Map<String, FlagSpec> declaredFlags = extractFlagDeclarations(obj);

    if (parsedFlags.containsKey("help") && !declaredFlags.containsKey("help")) {
      printUsage(declaredFlags, out);
      return null;
    }

    // Check for unrecognized flags.
    for (String flagName : parsedFlags.keySet()) {
      if (!declaredFlags.containsKey(flagName)) {
        throw new UnrecognizedFlagException(flagName);
      }
    }

    for (Map.Entry<String, String> flag : parsedFlags.entrySet()) {
      try {
        declaredFlags.get(flag.getKey()).setValue(flag.getValue());
      } catch (IllegalAccessException e) {
        throw new IllegalAccessError(e.getMessage());
      }
    }

    // Use environment variables to set values for flags that can be set from
    // the environment and weren't explicitly set on the command line.
    for (Map.Entry<String, FlagSpec> declFlag : declaredFlags.entrySet()) {
      FlagSpec flag = declFlag.getValue();
      if (flag.getEnvVar().length() > 0 && !parsedFlags.containsKey(flag.getName())) {
        String envVal = System.getenv(flag.getEnvVar());
        if (null != envVal) {
          // This environment variable was set.
          try {
            flag.setValue(envVal);
          } catch (IllegalAccessException e) {
            throw new IllegalAccessError(e.getMessage());
          }
        }
      }
    }

    return nonFlagArgs;
  }

  /**
   * Convenience method for init that prints usage to stdout.
   *
   * @param obj The object containing the flag definitions.
   * @param args The command-line arguments.
   */
  public static List<String> init(Object obj, String[] args) {
    return init(obj, args, System.out);
  }
}
