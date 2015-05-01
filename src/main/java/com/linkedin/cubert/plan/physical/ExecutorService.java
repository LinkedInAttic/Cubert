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

package com.linkedin.cubert.plan.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.utils.ExecutionConfig;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.ScriptStats;
import com.linkedin.cubert.utils.print;

/**
 * Parses and executes a phyiscal plan specified as a json script.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ExecutorService
{
    /* State of executing the script */
    public static enum ExecutionState
    {
        RUNNING, FINISHED
    }

    static final int MAX_PARALLEL_JOBS = 5;
    static final double PROGRESS_UPDATE = 0.04;

    /* The program json */
    private final JsonNode json;

    /* the total number of jobs */
    private int nJobs;

    /* Whether to run profiling on the job */
    private final boolean profileMode;

    /* Objects to gather execution statistics */
    private ScriptStats scriptStats;

    /* Scheduled Jobs maintain the list of jobs to be executed */
    final List<JobExecutor> scheduledJobs;

    /*
     * The logger thread is a separate thread that monitors the progress of the jobs and
     * reports the status to the user The thread mechanism is required to report combined
     * status of jobs running in parallel.
     */
    final LoggerThread loggerThread;

    /* jobsToLog retains the members which are currently running */
    final List<JobExecutor> jobsToLog;

    ExecutionState execState = ExecutionState.RUNNING;

    public ExecutorService(JsonNode json)
    {
        this.json = json;
        profileMode =
                json.has("profileMode") && json.get("profileMode").getBooleanValue();

        setupConf(this.json);
        try
        {
            scriptStats = new ScriptStats();
            scriptStats.init(json);
        }
        catch (Exception e)
        {
            scriptStats = null;
        }

        nJobs = json.get("jobs").size();

        scheduledJobs = new ArrayList<JobExecutor>(nJobs);
        jobsToLog = new ArrayList<JobExecutor>(nJobs);
        loggerThread = new LoggerThread(this);
    }

    private static void setupConf(JsonNode programNode)
    {

        // copy the hadoopConf and libjars from global level to each job
        JsonNode globalHadoopConf = programNode.get("hadoopConf");
        JsonNode libjars = programNode.get("libjars");

        for (JsonNode json : programNode.path("jobs"))
        {
            ObjectNode job = (ObjectNode) json;

            // if there isn't local hadoop conf, then use the global conf
            if (!job.has("hadoopConf"))
            {
                job.put("hadoopConf", globalHadoopConf);
            }
            else
            {
                // if there are local conf properties, then copy only those properties
                // from global properties that are not already defined at local level
                ObjectNode localHadoopConf = (ObjectNode) job.get("hadoopConf");
                Iterator<String> it = globalHadoopConf.getFieldNames();
                while (it.hasNext())
                {
                    String key = it.next();
                    if (!localHadoopConf.has(key))
                    {
                        localHadoopConf.put(key, globalHadoopConf.get(key));
                    }
                }
            }

            if (libjars != null)
                job.put("libjars", libjars);
        }
    }

    /**
     * Public API to execute all the jobs in the program. The jobs are executed serially
     * or in parallel decided on the ExecutionConfig method isParallelExec
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void execute() throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        try
        {
            loggerThread.setnJobsToExecute(nJobs);
            loggerThread.start();

            if (ExecutionConfig.getInstance().isParallelExec())
            {
                System.out.println("Executing jobs in parallel");
                new ThreadPoolManager(this, MAX_PARALLEL_JOBS, this.json).execute();
            }
            else
            {
                System.out.println("Executing jobs serially");
                for (int jobId = 0; jobId < nJobs; jobId++)
                {
                    executeJobId(jobId);
                }
            }
            finish();
            onCompletion();
        }
        finally
        {
            execState = ExecutionState.FINISHED;
        }
    }

    /**
     * Public API to execute a single job in the program.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void execute(int jobId) throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        loggerThread.setnJobsToExecute(1);
        try
        {
            loggerThread.start();
            executeJobId(jobId);
            finish();
        }
        finally
        {
            execState = ExecutionState.FINISHED;
        }
    }

    private void executeJobId(int jobId) throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        JsonNode job = json.get("jobs").get(jobId);
        JobExecutor jobExecutor = createJobExecutor(job);
        synchronized (scheduledJobs)
        {
            scheduledJobs.add(jobExecutor);
        }
        synchronized (jobsToLog)
        {
            jobsToLog.add(jobExecutor);
        }
        executeJob(jobExecutor);
    }

    JobExecutor createJobExecutor(JsonNode job) throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        print.f("Executing job [%s]....", JsonUtils.getText(job, "name"));

        if (job.has("jobType"))
        {
            String jobType = job.get("jobType").getTextValue();
            if (jobType.equals("GENERATE_DICTIONARY"))
            {
                return new DictionaryExecutor(job.toString(), profileMode);
            }
            else
            {
                throw new IllegalArgumentException("Job type " + jobType
                        + " is not recognized");
            }
        }
        else
        {
            return new JobExecutor(job.toString(), profileMode);
        }
    }

    void executeJob(JobExecutor jobExecutor) throws IOException,
            InterruptedException,
            ClassNotFoundException
    {
        scriptStats.setStartTime(jobExecutor.job);
        jobExecutor.run(false);
        print.f("Finished job [%s]....", JsonUtils.getText(jobExecutor.root, "name"));
        scriptStats.addJob(jobExecutor.job);
    }

    private void finish() throws IOException
    {
        /* Update user that the program execution is complete */
        System.out.println("100% complete");

        if (scriptStats != null)
        {
            scriptStats.computeAggregate();

            try
            {
                for (JobExecutor jobEx : scheduledJobs)
                {
                    if (jobEx.getJob().isSuccessful())
                    {
                        scriptStats.collectStatistics(jobEx.getJob());
                    }
                }
                scriptStats.printJobStats();
            }
            catch (Exception e)
            {
                System.err.println("Error in collecting job statistics. Err: " + e.getMessage());
            }
            scriptStats.printAggregate();
        }
    }

    private void onCompletion() throws IOException
    {
        if (json.has("onCompletion") && !json.get("onCompletion").isNull())
        {
            CompletionTasks.doCompletionTasks(json.get("onCompletion"));
        }
    }
}
