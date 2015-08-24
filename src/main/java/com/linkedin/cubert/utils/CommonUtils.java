/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.utils;

import java.util.List;
import java.util.Set;

public class CommonUtils
{
    private static int variableNameCounter = 1;

    public static String generateVariableName(Set<String> usedNames)
    {
        while (true)
        {
            String name = "var" + (variableNameCounter++);
            if (!usedNames.contains(name))
            {
                usedNames.add(name);
                return name;
            }
        }
    }

    public static boolean isPrefix(String[] array, String[] prefix)
    {
        if (array == null || prefix == null || array.length < prefix.length)
            return false;

        for (int i = 0; i < prefix.length; i++)
            if (!array[i].equals(prefix[i]))
                return false;

        return true;
    }

    public static boolean isPresent(String[] array, String item)
    {
        if (array == null || item == null)
            return false;
        for (String arrayItem : array)
        {
            if (arrayItem.equals(item))
                return true;
        }
        return false;
    }

    public static String[] multiply(String str, int times)
    {
        String[] array = new String[times];
        for (int i = 0; i < times; i++)
            array[i] = str;
        return array;
    }

    public static String[] zip(String[] a, String[] b)
    {
        return zip(a, b, "");
    }

    public static String[] zip(String[] a, String[] b, String sep)
    {
        String[] zip = new String[a.length];
        for (int i = 0; i < a.length; i++)
        {
            zip[i] = a[i] + sep + b[i];
        }

        return zip;
    }

    public static String join(String[] array, String sep)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(array[0]);
        for (int i = 1; i < array.length; i++)
        {
            builder.append(sep);
            builder.append(array[i]);
        }

        return builder.toString();
    }

    public static String[] prefix(String[] array, String prefix)
    {
        String[] result = new String[array.length];
        for (int i = 0; i < array.length; i++)
        {
            result[i] = prefix + array[i];
        }
        return result;
    }

    public static String[] decorate(String[] array)
    {
        String[] result = new String[array.length];
        for (int i = 0; i < array.length; i++)
        {
            result[i] = String.format("'%s'", array[i]);
        }
        return result;
    }

    public static String stripQuotes(String s)
    {
        return s.replaceAll("^\"|\"$|^\'|\'$", "");
    }

    public static String[] array(String str)
    {
        return new String[] { str };
    }

    public static String[] array(String... args)
    {
        return args;
    }

    public static String[] concat(String[] a, String[] b)
    {
        String[] result = new String[a.length + b.length];
        int idx = 0;
        for (int i = 0; i < a.length; i++)
        {
            result[idx++] = a[i];
        }
        for (int i = 0; i < b.length; i++)
        {
            result[idx++] = b[i];
        }
        return result;
    }

    public static String[] trim(String[] array)
    {
        for (int i = 0; i < array.length; i++)
            array[i] = array[i].trim();
        return array;
    }

    public static <T> int indexOfByRef(List<T> inList, T searchKey)
    {
        for (int i = 0; i < inList.size(); i++)
        {
            if (inList.get(i) == searchKey)
                return i;
        }
        return -1;
    }

    public static <T> String listAsString(List<T> inList)
    {
        return listAsString(inList, ",");
    }

    public static <T> String listAsString(List<T> inList, String separator)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        boolean isFirst = true;
        for (T listElement : inList)
        {
            sb.append((isFirst ? "" : separator) + listElement.toString());
            isFirst = false;
        }
        sb.append("]");
        return sb.toString();
    }
}
