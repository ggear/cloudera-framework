package com.cloudera.framework.testing;

import java.util.Map;

/**
 * Provide additional, convenience {@link org.junit.Assert} methods
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Assert extends org.junit.Assert {

  /**
   * Assert <code>actual</code> equals <code>expected</code>
   *
   * @param expected <code>Map<String, Map<Enum<?>, Long>></code>
   * @param actual   <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterEquals(Map expected, Map actual) {
    assertCounterEqualsLessThanGreaterThan(expected, actual, true, false, false, false);
  }

  /**
   * Assert <code>actual</code> less than <code>expected</code>
   *
   * @param expected <code>Map<String, Map<Enum<?>, Long>></code>
   * @param actual   <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterLessThan(Map expected, Map actual) {
    assertCounterEqualsLessThanGreaterThan(expected, actual, false, true, false, false);
  }

  /**
   * Assert <code>actual</code> greater than <code>expected</code>
   *
   * @param expected <code>Map<String, Map<Enum<?>, Long>></code>
   * @param actual   <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterGreaterThan(Map expected, Map actual) {
    assertCounterEqualsLessThanGreaterThan(expected, actual, false, false, true, false);
  }

  /**
   * Assert <code>actual</code> equals <code>expectedEquals</code>, less then
   * <code>expectedLessThan</code> and greater than
   * <code>expectedGreaterThan</code>
   *
   * @param expectedEquals      <code>Map<String, Map<Enum<?>, Long>></code>, may be empty
   * @param expectedLessThan    <code>Map<String, Map<Enum<?>, Long>></code>, may be empty
   * @param expectedGreaterThan <code>Map<String, Map<Enum<?>, Long>></code>, may be empty
   * @param actual              <code>Map<String, Map<Enum<?>, Long>></code>
   */
  public static void assertCounterEqualsLessThanGreaterThan(Map expectedEquals, Map expectedLessThan, Map expectedGreaterThan, Map actual) {
    assertCounterEqualsLessThanGreaterThan(expectedEquals, actual, true, false, false, true);
    assertCounterEqualsLessThanGreaterThan(expectedLessThan, actual, false, true, false, true);
    assertCounterEqualsLessThanGreaterThan(expectedGreaterThan, actual, false, false, true, true);
  }

  private static void assertCounterEqualsLessThanGreaterThan(Map expected, Map<String, Map<Enum<?>, Long>> actual, boolean assertEquals,
                                                             boolean assertLessThan, boolean assertGreaterThan, boolean isBatch) {
    if (!isBatch) {
      assertTrue("Expected smaller than actual", expected.size() >= actual.size());
    }
    for (String group : actual.keySet()) {
      if (!isBatch || expected.containsKey(group)) {
        assertTrue("Expected does not contain group [" + group + "]", expected.containsKey(group));
        if (!isBatch) {
          assertTrue("Expected smaller than actual for group [" + group + "]",
            ((Map<Enum<?>, Long>) expected.get(group)).size() >= actual.get(group).size());
        }
        for (Enum<?> counter : actual.get(group).keySet()) {
          if (!isBatch || ((Map<Enum<?>, Long>) expected.get(group)).containsKey(counter)) {
            assertTrue("Expected group [" + group + "] does not contain counter [" + counter + "]",
              ((Map<Enum<?>, Long>) expected.get(group)).containsKey(counter));
            Long expectedLong = ((Map<Enum<?>, Long>) expected.get(group)).get(counter);
            Long actualLong = actual.get(group).get(counter);
            if (assertEquals) {
              assertTrue("Expected [" + expectedLong + "] to be equal to actual [" + actualLong + "]", expectedLong.equals(actualLong));
            }
            if (assertLessThan) {
              assertTrue("Expected [" + expectedLong + "] to be greater than actual [" + actualLong + "]", expectedLong > actualLong);
            }
            if (assertGreaterThan) {
              assertTrue("Expected [" + expectedLong + "] to be less than actual [" + actualLong + "]", expectedLong < actualLong);
            }
          }
        }
      }
    }
  }

}
