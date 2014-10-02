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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.linkedin.cubert.io.SerializerUtils;

/**
 * Global configuration file which is maintained across all phases
 * 
 * Created by spyne on 6/26/14.
 */
public final class ExecutionConfig implements Serializable
{
    private static final long serialVersionUID = -7531002154245583955L;

    /* Constants */
    public static final String HADOOP_CONF_KEY = "EXECUTION_CONFIG";

    /* Config parameters: Default value required. */
    private boolean parallelExec = false;
    private String zkConnectionString = null;

    public boolean isParallelExec()
    {
        return parallelExec;
    }

    public String getZkConnectionString()
    {
        return zkConnectionString;
    }

    public void setParallelExec(boolean parallelExec)
    {
        this.parallelExec = parallelExec;
    }

    public void setZkConnectionString(String zkConnectionString)
    {
        this.zkConnectionString = zkConnectionString;
    }

    /**
     * APIs for Hadoop Config serialization and deserialization
     */
    public static void readConf(final Configuration conf) throws IOException
    {
        final String raw = conf.getRaw(HADOOP_CONF_KEY);
        if (raw == null)
        {
            return;
        }
        try
        {
            Loader.instance =
                    (ExecutionConfig) SerializerUtils.deserializeFromString(raw);
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    public static void writeConf(final Configuration conf) throws IOException
    {
        conf.set(HADOOP_CONF_KEY, SerializerUtils.serializeToString(getInstance()));
    }

    /**
     * Singleton management code
     * <p/>
     * Using a Loader to make the class initialization synchronized
     */
    private static class Loader
    {
        private static ExecutionConfig instance = new ExecutionConfig();
    }

    /* Disallow external object creation */
    private ExecutionConfig()
    {

    }

    /* Expose public API for getting an instance */
    public static ExecutionConfig getInstance()
    {
        return Loader.instance;
    }
}
