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

package com.linkedin.cubert.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.linkedin.cubert.io.rubix.RubixInputSplit;

@Deprecated
public class CombinedInputSplit<K> extends InputSplit implements Writable, Configurable
{
    List<RubixInputSplit<K, BytesWritable>> splits;
    Configuration conf;

    CombinedInputSplit()
    {

    }

    CombinedInputSplit(List<RubixInputSplit<K, BytesWritable>> splits)
    {
        this.splits = splits;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeByte(splits.size());
        for (RubixInputSplit<K, BytesWritable> split : splits)
        {
            split.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        int nsplits = in.readByte();
        splits = new ArrayList<RubixInputSplit<K, BytesWritable>>();
        for (int i = 0; i < nsplits; i++)
        {
            RubixInputSplit<K, BytesWritable> split =
                    new RubixInputSplit<K, BytesWritable>();
            split.setConf(conf);
            split.readFields(in);
            splits.add(split);
        }

    }

    @Override
    public long getLength() throws IOException,
            InterruptedException
    {
        long length = 0;
        for (RubixInputSplit<K, BytesWritable> split : splits)
        {
            length += split.getLength();
        }
        return length;
    }

    @Override
    public String[] getLocations() throws IOException,
            InterruptedException
    {
        return new String[] {};
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
    }

    @Override
    public Configuration getConf()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
