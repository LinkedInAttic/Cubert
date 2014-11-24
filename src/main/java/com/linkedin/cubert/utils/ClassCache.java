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

import java.util.HashMap;
import java.util.Map;

/**
 * Keeps a cache of class Objects for faster loads
 *
 * @author spyne
 */
public class ClassCache
{
    private static final Map<String, Class> classMap = new HashMap<String, Class>();

    public static Class<?> forName(String className) throws ClassNotFoundException
    {
        Class<?> c = classMap.get(className);
        if (c == null)
        {
            c = Class.forName(className);
            classMap.put(className, c);
        }
        return c;
    }
}