/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package io.cdap.plugin.scd2;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TimeParserTest {

  @Test
  public void testDurationParse() {
    Assert.assertEquals(TimeUnit.MINUTES.toMicros(5), TimeParser.parseDuration("5m"));
    Assert.assertEquals(TimeUnit.SECONDS.toMicros(5), TimeParser.parseDuration("5s"));
    Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(5), TimeParser.parseDuration("5ms"));
    Assert.assertEquals(TimeUnit.MICROSECONDS.toMicros(5), TimeParser.parseDuration("5us"));
    Assert.assertEquals(TimeUnit.HOURS.toMicros(5), TimeParser.parseDuration("5h"));
    Assert.assertEquals(TimeUnit.DAYS.toMicros(5), TimeParser.parseDuration("5d"));
    Assert.assertEquals(TimeUnit.DAYS.toMicros(5), TimeParser.parseDuration("+5d"));
    Assert.assertEquals(0 - TimeUnit.DAYS.toMicros(5), TimeParser.parseDuration("-5d"));

    try {
      TimeParser.parseDuration("5js");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      TimeParser.parseDuration("5a");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
