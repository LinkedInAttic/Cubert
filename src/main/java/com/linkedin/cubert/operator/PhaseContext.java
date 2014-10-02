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

package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.linkedin.cubert.utils.ExecutionConfig;

public class PhaseContext
{
    private static boolean initialized = false;
    private static Reducer.Context redContext;
    private static Mapper.Context mapContext;
    private static boolean isMapper;
    private static HashMap<String, Object> udfObjects = new HashMap<String, Object>();

    private static Configuration conf;

    public static void insertUDF(String udfName, Object udfObj)
    {
        udfObjects.put(udfName, udfObj);
    }

    public static Object getUDF(String udfName)
    {
        return udfObjects.get(udfName);
    }

    private PhaseContext()
    {
    }

    public static void create(Reducer.Context context, Configuration conf) throws IOException
    {
        redContext = context;
        isMapper = false;
        initCommonConfig(conf);
    }

    public static void create(Mapper.Context context, Configuration conf) throws IOException
    {
        mapContext = context;
        isMapper = true;
        initCommonConfig(conf);
    }

    private static void initCommonConfig(Configuration conf) throws IOException
    {
        initialized = true;
        PhaseContext.conf = conf;
        ExecutionConfig.readConf(getConf());
    }

    public static boolean isIntialized()
    {
        return initialized;
    }

    public static boolean isMapper()
    {
        return isMapper;
    }

    public static Mapper.Context getMapContext()
    {
        return mapContext;
    }

    public static Reducer.Context getRedContext()
    {
        return redContext;
    }

    public static Configuration getConf()
    {
        return conf;
    }

    public static TaskAttemptContext getContext()
    {
        if (isMapper)
            return mapContext;
        else
            return redContext;
    }

    public static Counter getCounter(String groupName, String counterName)
    {
        if (isMapper)
            return mapContext.getCounter(groupName, counterName);
        else
            return redContext.getCounter(groupName, counterName);
    }

    public static long getUniqueId()
    {
        if (isMapper)
            return PhaseContext.getMapContext().getTaskAttemptID().getTaskID().getId();
        else
            return PhaseContext.getRedContext().getTaskAttemptID().getTaskID().getId();
    }
}
