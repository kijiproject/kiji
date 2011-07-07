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

    // These two parameters may be controlled by explicit flags or by the environment.
    @Flag(name="home", envVar="HOME", hidden=true) public String homeVar = "somedefault";
    @Flag(name="missing", envVar="MISSING_ENV_VAR_XXXXXXX", hidden=true)
    public String missingEnv = "missing";

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

  private static class MySubclassedFlags extends MyFlags {
    @Flag public String flagSubclass;
  }

  private static class UnsupportedTypeFlags {
    @Flag private Object unsupportedFlagType;
  }

  private static class DuplicateFlagDeclaration {
    @Flag private int myFlag;
    @Flag(name="myFlag") private String myDuplicateFlag;
  }

  private static class HelpOverride {
    @Flag public boolean help;
    @Flag public String foo;
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
    Assert.assertEquals(
        "  --help=<boolean>\n\tDisplay this help message\n\t(Default=false)\n\n"
        + "  --flagDefault=<String>\n\t(Default=\"defaultValue\")\n\n"
        + "  --flagDouble=<double>\n\t(Default=0.0)\n\n"
        + "  --flagFloat=<float>\n\t(Default=0.0)\n\n"
        + "  --flagInt=<int>\n\t(Default=0)\n\n"
        + "  --flagLong=<long>\n\t(Default=0)\n\n"
        + "  --flagNoValue=<boolean>\n\ta boolean flag\n\t(Default=false)\n\n"
        + "  --flagOneDashBoolean=<boolean>\n\t(Default=false)\n\n"
        + "  --flagShort=<short>\n\t(Default=0)\n\n"
        + "  --flagString=<String>\n\t(Default=null)\n\n",
        help);
  }

  @Test
  public void testHelpOverride() {
    HelpOverride myFlags = new HelpOverride();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FlagParser.init(myFlags, new String[] {"--help", "--foo=bar"}, new PrintStream(out));

    // No usage info should have printed, since we declared our own help flag.
    Assert.assertTrue(out.toString().isEmpty());

    Assert.assertTrue(myFlags.help);
    Assert.assertTrue(myFlags.foo.equals("bar"));
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

  @Test
  public void testSubclassedFlags() {
    // Make sure the subclass inherits its superclass's flags.
    MySubclassedFlags myFlags = new MySubclassedFlags();
    Assert.assertNotNull(FlagParser.init(myFlags,
            new String[] {"--flagInt=7", "--flagSubclass=foo"}));

    Assert.assertEquals(7, myFlags.flagInt);
    Assert.assertEquals("foo", myFlags.flagSubclass);
  }

  @Test
  public void testEnvVarFlags() {
    MyFlags myFlags = new MyFlags();
    FlagParser.init(myFlags, new String[] { "--flagInt=7" });
    Assert.assertTrue(myFlags.homeVar.startsWith("/")); // should be some path.

    myFlags = new MyFlags();
    FlagParser.init(myFlags, new String[] { "--home=meep" });
    Assert.assertEquals("meep", myFlags.homeVar);

    myFlags = new MyFlags();
    FlagParser.init(myFlags, new String[] { "--missing=wombat" });
    Assert.assertEquals("wombat", myFlags.missingEnv);

    myFlags = new MyFlags();
    FlagParser.init(myFlags, new String[] { });
    Assert.assertEquals("missing", myFlags.missingEnv);
  }
}
