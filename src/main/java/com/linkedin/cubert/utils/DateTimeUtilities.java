/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */
package com.linkedin.cubert.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;



public class DateTimeUtilities
{
    public static DateTime getDateTime(String ts)
    {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime result = DateTime.parse(ts, formatter);
        return result;
    }

    public static DateTime getDateTime(String ts, String formatStr){
        DateTimeFormatter formatter = DateTimeFormat.forPattern(formatStr);
        DateTime result = DateTime.parse(ts, formatter);
        return result;
    }

    public static DateTime getDateTime(int ts){
      return getDateTime(Integer.toString(ts));
    }
    
    
    public static int asInt(DateTime dt)
    {
        return dt.year().get() * 100 * 100 + dt.monthOfYear().get() * 100
                + dt.dayOfMonth().get();
    }

    public static String getHyphenated(int dateInt)
    {
        DateTime dt = getDateTime(Integer.toString(dateInt));
        return String.format("%04d-%02d-%02d",
                             dt.year().get(),
                             dt.monthOfYear().get(),
                             dt.dayOfMonth().get());
    }
  

}
