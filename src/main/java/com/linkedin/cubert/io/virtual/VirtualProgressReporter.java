/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.io.virtual;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.linkedin.cubert.operator.PhaseContext;

/**
 * VirtualProgressReporter
 * This class helps report progress back to hadoop when using Virtual Storage to spawn mappers.
 *
 * While using Virtual Storage in Cubert, progress reporting on Hadoop will not work directly. In order to report
 * progress please use the setProgress method.
 *
 * Created by spyne on 2/13/15.
 */
public class VirtualProgressReporter
{
    /**
     * Set the progress of the current task.
     * *Note: Works only when using a Virtual Input Format
     *
     * @param value value of the progress must lie within [0.0, 1.0]
     */
    public static void setProgress(float value)
    {
        if (PhaseContext.isIntialized())
        {
            final Mapper.Context mapContext = PhaseContext.getMapContext();
            try
            {
                final FloatWritable progress = (FloatWritable) mapContext.getCurrentKey();
                progress.set(value);
                mapContext.nextKeyValue();
            }
            catch (Exception e)
            {
                System.err.println("Unable to report progress in Load Cyclic. Exception: " + e);
                e.printStackTrace();
            }
        }
    }
}
