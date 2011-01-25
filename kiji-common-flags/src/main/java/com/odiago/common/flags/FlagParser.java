// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FlagParser {

  /** Should not be constructed. */
  private FlagParser() {}

  /**
   * Pulls out all the fields that have been annotated with @Flag attributes.
   */
  private static Map<String, FlagSpec> extractFlagDeclarations(Object obj) {
    Map<String, FlagSpec> flags = new TreeMap<String, FlagSpec>();
    for (Field field : obj.getClass().getDeclaredFields()) {
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
    return flags;
  }

  /**
   * Parse the flags out of the command line arguments.  The non flag args are put into
   * nonFlagArgs.
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

  private static void printUsage(Map<String, FlagSpec> flags, PrintStream out) {
    final String FORMAT_STRING = "  --%s=<%s>\n%s\t(Default=%s)\n\n";
    if (!flags.containsKey("help")) {
      out.printf(FORMAT_STRING, "help", "boolean", "\tDisplay this help message\n", "false");
    }
    for (FlagSpec flag : flags.values()) {
      String usage = flag.getUsage();
      if (!usage.isEmpty()) {
        usage = "\t" + usage + "\n";
      }
      out.printf(FORMAT_STRING, flag.getName(), flag.getTypeName(), usage, flag.getDefaultValue());
    }
  }

  /**
   * Print the list of flags and their usage strings.
   */
  public static void printUsage(Object obj, PrintStream out) {
    printUsage(extractFlagDeclarations(obj), out);
  }

  /**
   * Parses the flags defined in obj out of args.  Prints usage and returns null
   * if the flags could not be parsed.  Otherwise it assigns the flag values to
   * the fields with @Flag declarations and returns the non-flag arguments.
   *
   * @param obj the instance of the class containing flag declarations.
   * @param args the command-line arguments.
   * @param out where to print usage info if there is a parsing error.
   *
   * @return the non-flag arguments, or null if the flags were not parsed.
   *
   * @throws DuplicateFlagException
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

    return nonFlagArgs;
  }

  /**
   * Convenience method for init that prints usage to stdout.
   */
  public static List<String> init(Object obj, String[] args) {
    return init(obj, args, System.out);
  }
}
