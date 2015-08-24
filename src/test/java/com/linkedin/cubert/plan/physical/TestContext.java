/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.plan.physical;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.security.Credentials;


/**
 * Dummy Context class to be used in TestOperators
 *
 * @author Mani Parkhe
 */
public class TestContext implements MapContext, ReduceContext
{
    @Override
    public InputSplit getInputSplit()
    {
        return null;
    }

    @Override
    public boolean nextKey()
        throws IOException, InterruptedException
    {
        return false;
    }

    @Override
    public Iterable<Object> getValues()
        throws IOException, InterruptedException
    {
        return null;
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException
    {
        return false;
    }

    @Override
    public Object getCurrentKey()
        throws IOException, InterruptedException
    {
        return null;
    }

    @Override
    public Object getCurrentValue()
        throws IOException, InterruptedException
    {
        return null;
    }

    @Override
    public void write(Object key, Object value)
        throws IOException, InterruptedException
    {

    }

    @Override
    public OutputCommitter getOutputCommitter()
    {
        return null;
    }

    @Override
    public TaskAttemptID getTaskAttemptID()
    {
        return null;
    }

    @Override
    public void setStatus(String msg)
    {

    }

    @Override
    public String getStatus()
    {
        return null;
    }

    @Override
    public float getProgress()
    {
        return 0;
    }

    private final HashMap<String, HashMap<String, Counter>> counterCache = new HashMap<String, HashMap<String, Counter>>();

    @Override
    public Counter getCounter(String groupName, String counterName)
    {
        HashMap<String, Counter> group = counterCache.get(groupName);
        if (group == null)
        {
            group = new HashMap<String, Counter>();
            counterCache.put(groupName, group);
        }

        Counter counter = group.get(counterName);
        if (counter == null)
        {
            counter = new GenericCounter(groupName, counterName);
            group.put(counterName, counter);
        }

        return counter;
    }

    @Override
    public Counter getCounter(Enum<?> counterName)
    {
        return getCounter("DEFAULT_GROUP", counterName.name());
    }

    @Override
    public Configuration getConfiguration()
    {
        return null;
    }

    @Override
    public Credentials getCredentials()
    {
        return null;
    }

    @Override
    public JobID getJobID()
    {
        return null;
    }

    @Override
    public int getNumReduceTasks()
    {
        return 0;
    }

    @Override
    public Path getWorkingDirectory()
        throws IOException
    {
        return null;
    }

    @Override
    public Class<?> getOutputKeyClass()
    {
        return null;
    }

    @Override
    public Class<?> getOutputValueClass()
    {
        return null;
    }

    @Override
    public Class<?> getMapOutputKeyClass()
    {
        return null;
    }

    @Override
    public Class<?> getMapOutputValueClass()
    {
        return null;
    }

    @Override
    public String getJobName()
    {
        return null;
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException
    {
        return null;
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException
    {
        return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException
    {
        return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException
    {
        return null;
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException
    {
        return null;
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException
    {
        return null;
    }

    @Override
    public RawComparator<?> getSortComparator()
    {
        return null;
    }

    @Override
    public String getJar()
    {
        return null;
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator()
    {
        return null;
    }

    @Override
    public RawComparator<?> getGroupingComparator()
    {
        return null;
    }

    @Override
    public boolean getJobSetupCleanupNeeded()
    {
        return false;
    }

    @Override
    public boolean getTaskCleanupNeeded()
    {
        return false;
    }

    @Override
    public boolean getProfileEnabled()
    {
        return false;
    }

    @Override
    public String getProfileParams()
    {
        return null;
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean isMap)
    {
        return null;
    }

    @Override
    public String getUser()
    {
        return null;
    }

    @Override
    public boolean getSymlink()
    {
        return false;
    }

    @Override
    public Path[] getArchiveClassPaths()
    {
        return new Path[0];
    }

    @Override
    public URI[] getCacheArchives()
        throws IOException
    {
        return new URI[0];
    }

    @Override
    public URI[] getCacheFiles()
        throws IOException
    {
        return new URI[0];
    }

    @Override
    public Path[] getLocalCacheArchives()
        throws IOException
    {
        return new Path[0];
    }

    @Override
    public Path[] getLocalCacheFiles()
        throws IOException
    {
        return new Path[0];
    }

    @Override
    public Path[] getFileClassPaths()
    {
        return new Path[0];
    }

    @Override
    public String[] getArchiveTimestamps()
    {
        return new String[0];
    }

    @Override
    public String[] getFileTimestamps()
    {
        return new String[0];
    }

    @Override
    public int getMaxMapAttempts()
    {
        return 0;
    }

    @Override
    public int getMaxReduceAttempts()
    {
        return 0;
    }

    @Override
    public void progress()
    {

    }
}
