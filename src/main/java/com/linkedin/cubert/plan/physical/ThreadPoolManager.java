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
import java.util.concurrent.Executors;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.plan.physical.ExecutorService.ExecutionState;

public class ThreadPoolManager
{

    private static final int SLEEP_TIME = 3000;

    private class JobRunnable implements Runnable
    {
        private final JobExecutor jobExecutor;

        public JobRunnable(JsonNode job) throws IOException,
                ClassNotFoundException,
                InstantiationException,
                IllegalAccessException
        {
            jobExecutor = executorService.createJobExecutor(job);
        }

        @Override
        public void run()
        {
            ThreadResponse response;

            try
            {
                executorService.executeJob(jobExecutor);
                response = new ThreadResponse(false, null);
            }
            // if any checked or unchecked exception occurs, thread needs to catch it
            catch (Throwable t)
            {
                response = new ThreadResponse(true, t);
            }

            // update job graphnode to finished
            graph.setJobToFinished(jobExecutor.root);

            // add ThreadResponse to list of responses
            synchronized (responses)
            {
                responses.add(response);
            }

            // notify main threadpool manager thread to wake up
            synchronized (conditionVariable)
            {
                conditionVariable.notify();
            }
        }
    }

    private class ThreadResponse
    {
        private final boolean threwException;
        private final Throwable t;

        public ThreadResponse(boolean threwException, Throwable t)
        {
            this.threwException = threwException;
            this.t = t;
        }
    }

    private final ExecutorService executorService;
    private final DependencyGraph graph = new DependencyGraph();
    private final java.util.concurrent.ExecutorService poolExecutor;
    private final Object conditionVariable = new Object();
    private final List<ThreadResponse> responses = new ArrayList<ThreadResponse>();

    ThreadPoolManager(ExecutorService executorService,
                      int numberOfParallelPrograms,
                      JsonNode program)
    {
        this.executorService = executorService;
        poolExecutor = Executors.newFixedThreadPool(numberOfParallelPrograms);
        setupGraph(program);
    }

    private void setupGraph(JsonNode programNode)
    {
        // need a list of jobNames in order to print the dependency graph jobs in
        // sequential order
        List<String> jobNames = new ArrayList<String>(programNode.path("jobs").size());

        for (JsonNode job : programNode.path("jobs"))
        {
            // get the name of the job
            String jobName = job.get("name").getTextValue();

            jobNames.add(jobName);

            // get the parents of the job
            List<String> parents = new ArrayList<String>();
            ArrayNode dependencies = (ArrayNode) job.get("dependsOn");
            JsonNode indexOfJob;
            for (JsonNode dependency : dependencies)
            {
                int index = dependency.getIntValue();
                indexOfJob = ((ArrayNode) programNode.get("jobs")).get(index);
                parents.add(indexOfJob.get("name").getTextValue());
            }

            // add the name, parents of the job, and the actual JsonNode job
            graph.addNode(jobName, parents, job);
        }
        // set the children field for parent job nodes
        graph.setChildren();

        // print graph
        System.out.println(graph.prettyPrint(jobNames));
    }

    // white: unscheduled, gray: scheduled, not yet finished, black: finished
    public void execute() throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            InterruptedException
    {
        // exceptionThrown set to true if a child thread or main thread throws an
        // exception
        boolean exceptionThrown = false;
        try
        {
            List<JsonNode> readyJobs;

            // continue looping until the graph is empty, indicating that all jobs have
            // finished execution
            while (graph.hasUnfinishedJobs())
            {
                readyJobs = graph.getReadyJobs();

                for (JsonNode job : readyJobs)
                {
                    // set node to gray, create a job thread, and add it to threadpool for
                    // execution
                    graph.setJobToScheduled(job);
                    JobRunnable thread = new JobRunnable(job);
                    synchronized (executorService.scheduledJobs)
                    {
                        executorService.scheduledJobs.add(thread.jobExecutor);
                    }
                    synchronized (executorService.jobsToLog)
                    {
                        executorService.jobsToLog.add(thread.jobExecutor);
                    }
                    poolExecutor.execute(thread);
                }
                synchronized (conditionVariable)
                {
                    // make sure no thread added a response and called notify() before
                    // main thread goes to sleep
                    if (responses.size() == 0)
                    {
                        conditionVariable.wait();
                    }
                }
                boolean threadExceptionOccurred = processThreadResponses();
                if (threadExceptionOccurred)
                {
                    exceptionThrown = true;
                    forceShutdown();
                    break;
                }
            }
        }
        // main thread exceptions, likely occurs when trying to configure a job
        catch (Exception e)
        {
            e.printStackTrace();
            exceptionThrown = true;
            forceShutdown();
        }
        finally
        {
            /*
             * enter finally block because an exception was thrown, or because we
             * successfully ran all jobs. in either case, we want to shutdown the
             * threadpool and its threads to finish
             */
            poolExecutor.shutdownNow();

            // if the main thread or a child thread threw an exception, exit with an error
            // code
            if (exceptionThrown)
            {
                executorService.execState = ExecutionState.FINISHED;
                System.exit(1);
            }
        }
    }

    private boolean processThreadResponses() throws IOException
    {
        boolean threadExceptionOccurred = false;
        synchronized (responses)
        {
            for (ThreadResponse response : responses)
            {
                if (response.threwException)
                {
                    // Log the exception
                    response.t.printStackTrace();
                    threadExceptionOccurred = true;
                }
            }
            responses.clear();
        }
        return threadExceptionOccurred;
    }

    private void forceShutdown() throws IOException,
            InterruptedException
    {
        Iterator<JobExecutor> itr;
        JobExecutor j;
        boolean failedToKillJob = false;

        // while not all the jobs have been killed
        while (true)
        {
            synchronized (executorService.scheduledJobs)
            {
                if (executorService.scheduledJobs.size() <= 0)
                {
                    break;
                }
            }
            if (failedToKillJob)
            {
                // waiting for job to get into RUNNING state before trying to kill it
                // again
                Thread.sleep(SLEEP_TIME);
            }

            synchronized (executorService.scheduledJobs)
            {
                itr = executorService.scheduledJobs.iterator();
                while (itr.hasNext())
                {
                    j = itr.next();
                    try
                    {
                        j.job.killJob();
                        itr.remove();
                    }
                    // Job is in DEFINE state, try to kill it again when it is RUNNING
                    catch (IllegalStateException e)
                    {
                        failedToKillJob = true;
                    }
                }
            }
        }
    }
}
