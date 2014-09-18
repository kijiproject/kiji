// (c) Copyright 2010 Odiago, Inc.

package com.odiago.common.flags;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestFlagParser {

  private static class MyFlags {
    @Flag(usage="a boolean flag") private boolean flagNoValue;
    @Flag protected boolean flagOneDashBoolean;
    @Flag(name="flagFloat") public float flagNumber;
    @Flag public double flagDouble;
    @Flag public int flagInt;
    @Flag public long flagLong;
    @Flag public short flagShort;
    @Flag public String flagString;
    @Flag public String flagDefault = "defaultValue";

    private String notAFlag;

    public boolean getFlagNoValue() {
      return flagNoValue;
    }

    public boolean getFlagOneDashBoolean() {
      return flagOneDashBoolean;
    }

    public float getFlagFloat() {
      return flagNumber;
    }
  }

  private static class UnsupportedTypeFlags {
    @Flag private Object unsupportedFlagType;
  }

  private static class DuplicateFlagDeclaration {
    @Flag private int myFlag;
    @Flag(name="myFlag") private String myDuplicateFlag;
  }

  @Test
  public void testParse() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "nonFlagArg1",
      "--flagNoValue",
      "-flagOneDashBoolean=false",
      "nonFlagArg2",
      "--flagFloat=-10.234",
      "--flagDouble=0.1",
      "--flagInt=-3",
      "--flagShort=10",
      "--flagLong=123",
      "--flagString=foo",
    };
    List<String> nonFlagArgs = FlagParser.init(myFlags, args);

    Assert.assertTrue(myFlags.getFlagNoValue());
    Assert.assertFalse(myFlags.getFlagOneDashBoolean());
    Assert.assertEquals(-10.234f, myFlags.getFlagFloat(), 0.0001f);
    Assert.assertEquals(0.1, myFlags.flagDouble, 0.0001f);
    Assert.assertEquals(-3, myFlags.flagInt);
    Assert.assertEquals(10, myFlags.flagShort);
    Assert.assertEquals(123, myFlags.flagLong);
    Assert.assertTrue(myFlags.flagString.equals("foo"));
    Assert.assertTrue(myFlags.flagDefault.equals("defaultValue"));

    Assert.assertEquals(2, nonFlagArgs.size());
    Assert.assertTrue(nonFlagArgs.get(0).equals("nonFlagArg1"));
    Assert.assertTrue(nonFlagArgs.get(1).equals("nonFlagArg2"));
  }

  @Test
  public void testUnrecognizedFlag() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "nonFlagArg1",
      "--flagNoValue",
      "-flagOneDashBoolean=false",
      "nonFlagArg2",
      "--flagFloat=-10.234",
      "--notAFlag=foo",
    };
    try {
      FlagParser.init(myFlags, args);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (UnrecognizedFlagException e) {
      Assert.assertTrue(e.getMessage().contains("notAFlag"));
    }
  }

  @Test
  public void testUnsupportedFlagType() {
    UnsupportedTypeFlags myFlags = new UnsupportedTypeFlags();
    String[] args = new String[] {
      "--unsupportedFlagType=null",
    };
    try {
      FlagParser.init(myFlags, args);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (UnsupportedFlagTypeException e) {
      Assert.assertTrue(e.getMessage().contains("unsupportedFlagType"));;
    }
  }

  @Test
  public void testIllegalFlagValue() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "--flagFloat=notANumber",
    };
    try {
      FlagParser.init(myFlags, args);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (IllegalFlagValueException e) {
      Assert.assertTrue(e.getMessage().contains("flagFloat"));
    }
  }

  @Test
  public void testHexInt() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "--flagInt=0xA",
    };
    try {
      FlagParser.init(myFlags, args);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (IllegalFlagValueException e) {
      // Parsing hex is not supported.
      Assert.assertTrue(e.getMessage().contains("flagInt"));
    }
  }

  @Test
  public void testNoEqualsSeparator() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "--flagInt",
      "5",
    };
    try {
      FlagParser.init(myFlags, args);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (IllegalFlagValueException e) {
      // This will be treated as a flag without a value.
      Assert.assertTrue(e.getMessage().contains("flagInt"));
    }
  }

  @Test
  public void testDuplicateFlags() {
    DuplicateFlagDeclaration myFlags = new DuplicateFlagDeclaration();
    try {
      FlagParser.init(myFlags, new String[] {});
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (DuplicateFlagException e) {
      Assert.assertTrue(e.getMessage().contains("myFlag"));
    }
  }

  @Test
  public void testPrintUsage() {
    MyFlags myFlags = new MyFlags();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FlagParser.init(myFlags, new String[] {"--help"}, new PrintStream(out));
    String help = out.toString();
    Assert.assertTrue(help.equals(
        "  --help=<boolean>\n\tDisplay this help message\n\n"
        + "  --flagDefault=<String>\n\t\n"
        + "  --flagDouble=<double>\n\t\n"
        + "  --flagFloat=<float>\n\t\n"
        + "  --flagInt=<int>\n\t\n"
        + "  --flagLong=<long>\n\t\n"
        + "  --flagNoValue=<boolean>\n\ta boolean flag\n\n"
        + "  --flagOneDashBoolean=<boolean>\n\t\n"
        + "  --flagShort=<short>\n\t\n"
        + "  --flagString=<String>\n\t\n"));
  }

  @Test
  public void testIgnoreAfterDoubleDashMarker() {
    MyFlags myFlags = new MyFlags();
    FlagParser.init(myFlags, new String[] {"--flagInt=7", "--", "--flagDefault=foo"});

    Assert.assertEquals(7, myFlags.flagInt);
    // flagDefault should not have changed, since it was after the "--".
    Assert.assertEquals("defaultValue" , myFlags.flagDefault);
  }

  @Test
  public void testKeepLatestFlag() {
    MyFlags myFlags = new MyFlags();
    FlagParser.init(myFlags, new String[] {"--flagInt=7", "--flagInt=8"});

    // Keeps the last flag value.
    Assert.assertEquals(8, myFlags.flagInt);
  }
}
