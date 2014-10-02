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
import java.util.Map;

public class print
{
    public static void f(String format, Object... args)
    {
        System.out.println(String.format(format, args));
    }

    public static <E> void l(List<E> list)
    {
        StringBuilder sb = new StringBuilder();

        for (E item : list)
        {
            sb.append(item);
            sb.append("\n");
        }

        System.out.println(sb.toString());
    }

    public static <K, V> void m(Map<K, V> map)
    {
        for (Map.Entry<K, V> entry : map.entrySet())
        {
            System.out.println(entry.getKey() + " => " + entry.getValue());
        }
    }
}
