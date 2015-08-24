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

import com.linkedin.cubert.utils.ExecutionConfig;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRTaskContext;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class PhaseContext
{
    private static boolean initialized = false;
    private static ReduceContext redContext;
    private static MapContext mapContext;
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

    public static void create(ReduceContext context, Configuration conf) throws IOException
    {
        redContext = context;
        isMapper = false;
        initCommonConfig(conf);
        PigStatusReporter.getInstance().setContext(new MRTaskContext(context));
    }

    public static void create(MapContext context, Configuration conf) throws IOException
    {
        mapContext = context;
        isMapper = true;
        initCommonConfig(conf);
        PigStatusReporter.getInstance().setContext(new MRTaskContext(context));
    }

    private static void initCommonConfig(Configuration conf) throws IOException
    {
        PhaseContext.conf = conf;
        ExecutionConfig.readConf(getConf());
        UDFContext.getUDFContext().addJobConf(conf);
        initialized = true;
    }

    public static boolean isIntialized()
    {
        return initialized;
    }

    public static boolean isMapper()
    {
        return isMapper;
    }

    public static MapContext getMapContext()
    {
        return mapContext;
    }

    public static ReduceContext getRedContext()
    {
        return redContext;
    }

    public static Configuration getConf()
    {
        return conf;
    }

    public static TaskInputOutputContext getContext()
    {
        if (isMapper)
            return mapContext;
        else
            return redContext;
    }

    public static Counter getCounter(String groupName, String counterName)
    {
        return getContext().getCounter(groupName, counterName);
    }

    public static long getUniqueId()
    {
        return  getContext().getTaskAttemptID().getTaskID().getId();
    }
}
