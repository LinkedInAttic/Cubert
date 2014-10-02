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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MultiMapperSplit extends InputSplit implements Writable, Configurable
{
    private Configuration conf;
    private InputSplit actualSplit;
    private int multiMapperIndex;

    public MultiMapperSplit()
    {

    }

    public MultiMapperSplit(InputSplit actualSplit, int multiMapperIndex)
    {
        this.actualSplit = actualSplit;
        this.multiMapperIndex = multiMapperIndex;
    }

    @Override
    public long getLength() throws IOException,
            InterruptedException
    {
        return actualSplit.getLength();
    }

    @Override
    public String[] getLocations() throws IOException,
            InterruptedException
    {
        return actualSplit.getLocations();
    }

    public InputSplit getActualSplit()
    {
        return actualSplit;
    }

    public int getMultiMapperIndex()
    {
        return multiMapperIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(multiMapperIndex);

        if (actualSplit instanceof FileSplit)
        {
            out.writeBoolean(true);
            FileSplit fileSplit = (FileSplit) actualSplit;
            Text.writeString(out, fileSplit.getPath().toString());
            out.writeLong(fileSplit.getStart());
            out.writeLong(fileSplit.getLength());
        }
        else if (actualSplit instanceof Writable)
        {
            out.writeBoolean(false);
            Text.writeString(out, actualSplit.getClass().getName());
            ((Writable) actualSplit).write(out);
        }
        else
        {
            throw new IOException("Input Split class " + actualSplit.getClass()
                    + " cannot be serialized");
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        multiMapperIndex = in.readInt();

        // patch the conf to this multiMapperIndex
        ConfigurationDiff confDiff = new ConfigurationDiff(conf);
        confDiff.applyDiff(multiMapperIndex);

        boolean isFileSplit = in.readBoolean();
        if (isFileSplit)
        {
            Path file = new Path(Text.readString(in));
            long start = in.readLong();
            long length = in.readLong();

            actualSplit = new FileSplit(file, start, length, null);
        }
        else
        {
            String actualSplitClass = Text.readString(in);
            try
            {
                actualSplit =
                        Class.forName(actualSplitClass)
                             .asSubclass(InputSplit.class)
                             .newInstance();
                if (actualSplit instanceof Configurable)
                    ((Configurable) actualSplit).setConf(conf);

                ((Writable) actualSplit).readFields(in);

            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public String toString()
    {
        return String.format("%s (%s) [%d]",
                             actualSplit.toString(),
                             actualSplit.getClass(),
                             multiMapperIndex);
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
    }

    @Override
    public Configuration getConf()
    {
        return null;
    }
}
