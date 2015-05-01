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

package com.linkedin.cubert.io.virtual;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * The input splits for the Virtual storage.
 * 
 * @author Vinitha Gankidi
 * 
 * @param <K>
 * @param <V>
 */
public class VirtualInputSplit<K, V> extends InputSplit implements Writable
{
    @Override
    public void write(DataOutput out) throws IOException
    {
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
    }

    @Override
    public long getLength() throws IOException,
            InterruptedException
    {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException,
            InterruptedException
    {
        return new String[] {};
    }

}
