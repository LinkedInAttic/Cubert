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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

public class MemoryStats
{
    public static void print(String msg)
    {
        final int mb = 1024 * 1024;

        Runtime runtime = Runtime.getRuntime();

        System.out.println(String.format("### %s %.2fMB used, %.2fMB free, %.2fMB total, %.2fMB max",
                                         msg,
                                         1.0
                                                 * (runtime.totalMemory() - runtime.freeMemory())
                                                 / mb,
                                         1.0 * runtime.freeMemory() / mb,
                                         1.0 * runtime.totalMemory() / mb,
                                         1.0 * runtime.maxMemory() / mb));

    }

    public static void printGCStats()
    {
        long totalGarbageCollections = 0;
        long garbageCollectionTime = 0;

        for(GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
        {

            long count = gc.getCollectionCount();

            if(count >= 0)
            {
                totalGarbageCollections += count;
            }

            long time = gc.getCollectionTime();

            if(time >= 0)
            {
                garbageCollectionTime += time;
            }
        }

        print.f("MemoryStats: #GC calls: %d Total GC Time: %d ms", totalGarbageCollections, garbageCollectionTime);
    }
}
