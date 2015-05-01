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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonNode;

/**
 * Obtain statistics of Hadoop Jobs
 *
 * Created by spyne on 6/25/14.
 */
public class ScriptStats
{
    public static class TaskStats
    {
        final TaskID id;
        final long startTime, endTime;
        final Counters counters;
        final static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");

        public TaskStats(TaskReport report)
        {
            id = report.getTaskID();
            this.startTime = report.getStartTime();
            this.endTime = report.getFinishTime();
            this.counters = new Counters(report.getCounters());
        }

        public long getDuration()
        {
            return endTime - startTime;
        }

        public static String getHumanReadableTime(long time)
        {
            return getTimeDifference(time, 0);
        }

        public static String getTimeDifference(long endTime, long startTime)
        {
            return StringUtils.getFormattedTimeWithDiff(dateFormat, endTime, startTime);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("[\nTask ID: ").append(id);
            sb.append(" Start Time: ").append(getHumanReadableTime(startTime));
            sb.append(" End Time: ").append(getTimeDifference(endTime, startTime));
            sb.append("\n").append(counters).append("\n]");
            return sb.toString();
        }
    }

    public static class Stats
    {
        long startTime, endTime;
        Counters counters;

        long minMapTime    = -1;
        long maxMapTime    = -1;
        long avgMapTime    = -1;
        long medianMapTime = -1;

        long minReduceTime     = -1;
        long maxReduceTime     = -1;
        long avgReduceTime     = -1;
        long medianReduceTime  = -1;

        List<TaskStats> mapTasks, reduceTasks;

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

    /**
     * Collect statistics for successful jobs
     *
     * @param job The job object
     * @return a stats object
     * @throws java.io.IOException
     */
    public Stats collectStatistics(final Job job) throws IOException
    {
        final JobID jobID = JobID.forName(job.getJobID().toString());
        Stats stats = getStats(job);
        TaskReport[] mapTaskReports = jobClient.getMapTaskReports(jobID);
        TaskReport[] reduceTaskReports = jobClient.getReduceTaskReports(jobID);

        /**
         * Collecting for map tasks
         */
        List<Long> durations = new ArrayList<Long>();
        List<TaskStats> taskStatslist = new ArrayList<TaskStats>();
        long total = processTaskReports(mapTaskReports, durations, taskStatslist);

        Collections.sort(durations);
        int size = durations.size();

        if (size > 0)
        {
            stats.mapTasks = taskStatslist;

            stats.minMapTime = durations.get(0);
            stats.maxMapTime = durations.get(size - 1);
            stats.avgMapTime = total / size;
            stats.medianMapTime = median(durations);
        }

        /* reset aggregates */
        taskStatslist = new ArrayList<TaskStats>();
        durations.clear();

        /**
         * Collecting for reduce tasks
         */
        total = processTaskReports(reduceTaskReports, durations, taskStatslist);
        Collections.sort(durations);
        size = durations.size();

        if (size > 0)
        {
            stats.reduceTasks = taskStatslist;

            stats.minReduceTime = durations.get(0);
            stats.maxReduceTime = durations.get(size - 1);
            stats.avgReduceTime = total / size;
            stats.medianReduceTime = median(durations);
        }

        return stats;
    }

    public long processTaskReports(TaskReport[] taskReports, List<Long> durations, List<TaskStats> taskStatslist)
    {
        long total = 0;
        for (TaskReport report : taskReports)
        {
            if (report.getCurrentStatus() == TIPStatus.COMPLETE)
            {
                TaskStats ts = new TaskStats(report);
                taskStatslist.add(ts);
                durations.add(ts.getDuration());
                total += ts.getDuration();
            }
        }
        return total;
    }

    private long median(List<Long> numbers)
    {
        final int size = numbers.size();
        if (size == 0) return -1;
        else return size % 2 == 1? numbers.get(size / 2) : (numbers.get(size / 2) + numbers.get((size / 2) - 1)) / 2;
    }

    public Stats getAggregate()
    {
        return aggregate;
    }

    public void printAggregate()
    {
        System.out.println("Aggregated Hadoop Counters for the Cubert Job:");
        System.out.println(getAggregate());
        System.out.println();
    }

    public void printJobStats()
    {
        print.f("Statistics of individual Jobs");
        print.f("--------------------------------------------");
        print.f("%-30s %15s %15s %15s %20s %15s %15s %15s %20s",
                "Job Name",
                "minMapperTime", "maxMapperTime", "avgMapperTime", "medianMapperTime",
                "minReducerTime", "maxReducerTime", "avgReducerTime", "medianReducerTime");
        for (Map.Entry<Job, Stats> e : statsMap.entrySet())
        {
            final Job j = e.getKey();
            final Stats v = e.getValue();
            print.f("%-30s %15d %15d %15d %20d %15d %15d %15d %20d",
                    j.getJobID(),
                    v.minMapTime, v.maxMapTime, v.avgMapTime, v.medianMapTime,
                    v.minReduceTime, v.maxReduceTime, v.avgReduceTime, v.medianReduceTime);
        }
        System.out.println();
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
