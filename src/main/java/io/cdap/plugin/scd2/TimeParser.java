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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.concurrent.TimeUnit;

/**
 * Parses time expressions.
 */
public class TimeParser {

  /**
   * Parses a duration String to its long value in microseconds.
   * Frequency string consists of a number followed by an unit, with 'us' for microseconds, 'ms' for milliseconds,
   * 's' for seconds, 'm' for minutes, 'h' for hours and 'd' for days.
   * For example, an input of '5m' means 5 minutes which will be parsed to 300000000 microseconds.
   *
   * @param durationStr the duration string (ex: 5m, 5h etc).
   * @return microseconds equivalent of the duration string
   */
  public static long parseDuration(String durationStr) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(durationStr) && durationStr.length() >= 2);
    durationStr = durationStr.trim().toLowerCase();

    TimeUnit unit;

    char lastChar = durationStr.charAt(durationStr.length() - 1);
    switch (lastChar) {
      case 's':
        char otherChar = durationStr.charAt(durationStr.length() - 2);
        switch (otherChar) {
          case 'm':
            unit = TimeUnit.MILLISECONDS;
            break;
          case 'u':
            unit = TimeUnit.MICROSECONDS;
            break;
          default:
            if (!(otherChar >= '0' && otherChar <= '9')) {
              throw new IllegalArgumentException(String.format("Time unit not supported: %s", otherChar + lastChar));
            }
            unit = TimeUnit.SECONDS;
        }
        break;
      case 'm':
        unit = TimeUnit.MINUTES;
        break;
      case 'h':
        unit = TimeUnit.HOURS;
        break;
      case 'd':
        unit = TimeUnit.DAYS;
        break;
      default:
        throw new IllegalArgumentException(String.format("Time unit not supported: %s", lastChar));
    }

    String value = (unit.equals(TimeUnit.MICROSECONDS) || unit.equals(TimeUnit.MILLISECONDS)) ?
                     durationStr.substring(0, durationStr.length() - 2) :
                     durationStr.substring(0, durationStr.length() - 1);

    try {
      return unit.toMicros(Long.parseLong(value));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Error parsing the duration string %s. Cannot parse %s as a long",
                      durationStr, value));
    }
  }
}
