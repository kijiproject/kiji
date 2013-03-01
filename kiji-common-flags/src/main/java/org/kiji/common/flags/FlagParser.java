/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  WibiData, Inc.
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

package org.kiji.common.flags;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

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
    final Map<String, FlagSpec> flags = new TreeMap<String, FlagSpec>();
    // Walk up the chain of inheritance:
    for (Class<?> clazz = obj.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      // Register public fields with @Flag annotations:
      for (Field field : clazz.getDeclaredFields()) {
        final Flag flag = field.getAnnotation(Flag.class);
        if (flag != null) {
          final FlagSpec flagSpec = new FlagSpec(field, flag, obj);
          final String flagName = flagSpec.getName();
          if (flags.containsKey(flagName)) {
            throw new DuplicateFlagException(flagName);
          }
          flags.put(flagSpec.getName(), flagSpec);
        }
      }
    }
    return flags;
  }

  /** Pattern matching a flag argument. */
  private static final Pattern FLAG_RE = Pattern.compile("^--?([^=]*)(=(.*))?$");

  /** Symbol used to delimit the end of flags to parse. */
  private static final String END_FLAG_SYMBOL = "--";

  /** Name of implicit help flag name. */
  private static final String HELP_FLAG_NAME = "help";

  /**
   * Parse the flags out of the command line arguments.  The non flag args are put into
   * nonFlagArgs.
   *
   * @param args The arguments to parse.
   * @param nonFlagArgs The remaining non-flag arguments.
   * @param declaredFlags Declared flag map.
   * @param ignoreUnknownFlags When set, unknown flags behave like non-flag arguments.
   *
   * @return A map from flag-name to the list of all values specified for the flag, in order.
   *
   * @throws UnrecognizedFlagException when encountering an unknown flag name while
   *     ignoreUnknownFlags is not set.
   */
  private static ListMultimap<String, String> parseArgs(
      String[] args,
      List<String> nonFlagArgs,
      Map<String, FlagSpec> declaredFlags,
      boolean ignoreUnknownFlags)
      throws UnrecognizedFlagException {

    final ListMultimap<String, String> parsedFlags = ArrayListMultimap.create();
    boolean ignoreTheRest = false;
    for (String arg : args) {
      if (ignoreTheRest) {
        nonFlagArgs.add(arg);
        continue;
      }
      if (arg.equals(END_FLAG_SYMBOL)) {
        // Ignore all arguments after this special symbol:
        ignoreTheRest = true;
        continue;
      }
      final Matcher matcher = FLAG_RE.matcher(arg);
      if (!matcher.matches()) {
        // Non-flag argument:
        nonFlagArgs.add(arg);
        continue;
      }
      final String flagName = matcher.group(1);
      final String flagValue = matcher.group(3);  // may be null

      if (declaredFlags.containsKey(flagName) || flagName.equals(HELP_FLAG_NAME)) {
        parsedFlags.put(flagName, flagValue);
      } else if (ignoreUnknownFlags) {
        // Flag argument but unknown flag name:
        nonFlagArgs.add(arg);
      } else {
        throw new UnrecognizedFlagException(flagName);
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
   * @param ignoreUnknownFlags When set, unknown flags behave like non-flag arguments.
   *
   * @return The non-flag arguments, or null if the flags were not parsed.
   *
   * @throws DuplicateFlagException If there are duplicate flags.
   * @throws UnrecognizedFlagException When parsing a flag that was not declared.
   */
  public static List<String> init(
      Object obj,
      String[] args,
      PrintStream out,
      boolean ignoreUnknownFlags) {

    final List<String> nonFlagArgs = new ArrayList<String>();
    final Map<String, FlagSpec> declaredFlags = extractFlagDeclarations(obj);
    final ListMultimap<String, String> parsedFlags =
        parseArgs(args, nonFlagArgs, declaredFlags, ignoreUnknownFlags);

    if (parsedFlags.containsKey("help") && !declaredFlags.containsKey("help")) {
      printUsage(declaredFlags, out);
      return null;
    }

    try {
      // Always walk through all command-line flags:
      for (Map.Entry<String, FlagSpec> entry : declaredFlags.entrySet()) {
        final String flagName = entry.getKey();
        final FlagSpec spec = entry.getValue();

        final List<String> values = parsedFlags.get(flagName);
        if (!values.isEmpty()) {
          // Flag is specified once or more on the command-line:
          spec.setValue(values);
        } else if (!spec.getEnvVar().isEmpty()) {
          final String envVal = System.getenv(spec.getEnvVar());
          if (null != envVal) {
            // Flag is not specified on the command-line but is set through environment variable:
            spec.setValue(Lists.newArrayList(envVal));
          }
        } else {
          // Flag is not specified on the command-line:
          spec.setValue(Collections.<String>emptyList());
        }
      }
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }

    return nonFlagArgs;
  }

  /**
   * Convenience method for init that throws an exception when encountering unknown flags.
   *
   * @param obj The instance of the class containing flag declarations.
   * @param args The command-line arguments.
   * @param out Where to print usage info if there is a parsing error.
   *
   * @return The non-flag arguments, or null if the flags were not parsed.
   *
   * @throws DuplicateFlagException If there are duplicate flags.
   * @throws UnrecognizedFlagException When parsing a flag that was not declared.
   */
  public static List<String> init(Object obj, String[] args, PrintStream out) {
    return init(obj, args, out, false);
  }

  /**
   * Convenience method for init that prints usage to stdout.
   *
   * @param obj The object containing the flag definitions.
   * @param args The command-line arguments.
   *
   * @throws DuplicateFlagException If there are duplicate flags.
   * @throws UnrecognizedFlagException When parsing a flag that was not declared.
   */
  public static List<String> init(Object obj, String[] args) {
    return init(obj, args, System.out);
  }
}
