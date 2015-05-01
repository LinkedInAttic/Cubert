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

/**
 * This class represents the Logger thread that presents the program status to the user.
 *
* Created by spyne on 9/26/14.
*/
class LoggerThread extends Thread
{
    final ExecutorService executorService;

    /* The total number of jobs to execute */
    private Integer nJobsToExecute = null;

    public LoggerThread(ExecutorService executorService)
    {
        this.executorService = executorService;
    }

    @Override
    public void run()
    {
        try
        {
            logProgress();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void logProgress() throws IOException,
            InterruptedException
    {
        double lastProgress = 0;
        while (executorService.execState == ExecutorService.ExecutionState.RUNNING)
        {
            // 500 ms is what Pig uses to let hadoop jobs progress before computing
            // the latest progress
            Thread.sleep(500);

            logTrackingURLs();

            double newProgress = computeLatestProgress() / nJobsToExecute;
            if (notifyProgress(newProgress, lastProgress))
            {
                lastProgress = newProgress;
            }
        }
    }

    private void logTrackingURLs()
    {
        synchronized (executorService.jobsToLog)
        {
            JobExecutor j;
            Iterator<JobExecutor> itr = executorService.jobsToLog.iterator();
            while (itr.hasNext())
            {
                j = itr.next();
                try
                {
                    String url = j.job.getTrackingURL();
                    System.out.println("Job: [" + j.job.getJobName()
                            + "], More information at: " + url);
                    itr.remove();
                }
                // job in define state, not yet running, no URL
                catch (IllegalStateException ignored) {}
            }
        }
    }

    /* returns value between 0.0 and number of jobs */
    private double computeLatestProgress() throws IOException
    {
        double retval = 0.0;
        retval += getSuccessfulJobsSize();
        List<JobExecutor> runningJobs = getRunningJobs();

        for (JobExecutor runningJob : runningJobs)
        {
            retval += progressOfRunningJob(runningJob);
        }
        return retval;
    }

    private int getSuccessfulJobsSize() throws IOException
    {
        int retval = 0;

        synchronized (executorService.scheduledJobs)
        {
            for (JobExecutor scheduledJob : executorService.scheduledJobs)
            {
                try
                {
                    if (scheduledJob.job.isSuccessful())
                    {
                        retval++;
                    }
                }
                catch (IllegalStateException ignored) {}
            }
        }
        return retval;
    }

    private List<JobExecutor> getRunningJobs() throws IOException
    {
        List<JobExecutor> runningJobs = new ArrayList<JobExecutor>();

        synchronized (executorService.scheduledJobs)
        {
            for (JobExecutor scheduledJob : executorService.scheduledJobs)
            {
                try
                {
                    if (!scheduledJob.job.isComplete())
                    {
                        runningJobs.add(scheduledJob);
                    }
                }
                catch (IllegalStateException ignored) {}
            }
        }
        return runningJobs;
    }

    private double progressOfRunningJob(JobExecutor jobexecutor) throws IOException
    {
        double retval = 0.0;
        try
        {
            retval += jobexecutor.job.mapProgress();
            if (jobexecutor.hasReducePhase())
            {
                retval += jobexecutor.job.reduceProgress();
                retval /= 2;
            }
            return retval;
        }
        catch (IllegalStateException e)
        {
            return 0.0;
        }
    }

    private boolean notifyProgress(double newProgress, double lastProgress)
    {
        if (newProgress >= (lastProgress + ExecutorService.PROGRESS_UPDATE))
        {
            int perCom = (int) (newProgress * 100);
            if (perCom != 100)
            {
                System.out.println(perCom + "% complete");
            }
            return true;
        }
        return false;
    }

    public void setnJobsToExecute(int nJobsToExecute)
    {
        this.nJobsToExecute = nJobsToExecute;
    }
}
