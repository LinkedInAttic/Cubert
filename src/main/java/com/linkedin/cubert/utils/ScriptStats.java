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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonNode;

/**
 * Obtain statistics of Hadoop Jobs
 * 
 * Created by spyne on 6/25/14.
 */
public class ScriptStats
{
    public static class Stats
    {
        long startTime, endTime;
        Counters counters;

        @Override
        public String toString()
        {
            return "Duration: " + (endTime - startTime) + " ms\n" + counters.toString();
        }
    }
    private final Configuration conf;
    private final JobClient jobClient;

    /* Map of jobID (String) vs mapred.Counters */
    private final Map<Job, Stats> statsMap = new HashMap<Job, Stats>();
    private final Stats aggregate = new Stats();

    public ScriptStats()
    {
        conf = new Configuration();
        try
        {
            jobClient = new JobClient(new JobConf(conf));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        aggregate.startTime = System.currentTimeMillis();
        aggregate.counters = new Counters();
    }

    public void init(JsonNode json)
    {
    }

    public org.apache.hadoop.mapred.Counters getCounters(String jobid) throws IOException
    {
        final JobID jobID = JobID.forName(jobid);
        RunningJob runningJob = jobClient.getJob(jobID);
        return runningJob == null ? null : runningJob.getCounters();
    }

    private Stats getStats(final Job job)
    {
        Stats stats = statsMap.get(job);
        if (stats == null)
        {
            stats = new Stats();
            statsMap.put(job, stats);
        }
        return stats;
    }

    public void setStartTime(final Job job)
    {
        getStats(job).startTime = System.currentTimeMillis();
    }

    public void addJob(final Job job)
    {
        getStats(job).endTime = System.currentTimeMillis();
        Counters counters;
        try
        {
            counters = job.getCounters();
        }
        catch (IOException e)
        {
            counters = null;
        }
        getStats(job).counters = counters;
    }

    public void computeAggregate()
    {
        for (Stats s : statsMap.values())
        {
            aggregate.counters.incrAllCounters(s.counters);
        }
        aggregate.endTime = System.currentTimeMillis();
    }

    public Stats getAggregate()
    {
        return aggregate;
    }

    public void printAggregate()
    {
        // for (Counters c : statsMap.values())
        // {
        // System.out.println(c);
        // }
        System.out.println("Aggregated Hadoop Counters for the Cubert Job:");
        System.out.println(getAggregate());
    }

    public static void main(String[] args) throws IOException
    {
        if (args.length == 0)
        {
            System.err.println("Usage: Please give a valid job ID as an argument");
            System.exit(0);
        }

        ScriptStats instance = new ScriptStats();
        final org.apache.hadoop.mapred.Counters counters = instance.getCounters(args[0]);
        if (counters != null)
            System.out.println(counters);
        else
            System.err.println("Unable to retrieve RunningJob for job ID: " + args[0]);
    }
}
