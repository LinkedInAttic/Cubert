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

package com.linkedin.cubert.io.rubix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.linkedin.cubert.io.rubix.RubixFile.KeyData;

public class RubixInputFormat<K, V> extends FileInputFormat<K, V>
{

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException
    {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        FileSystem.get(job.getConfiguration());

        for (FileStatus file : files)
        {
            Path path = file.getPath();
            RubixFile<K, V> rubixFile = new RubixFile<K, V>(job.getConfiguration(), path);

            List<KeyData<K>> keyDataList;
            try
            {
                keyDataList = rubixFile.getKeyData();
                for (KeyData<K> keyData : keyDataList)
                {
                    InputSplit split =
                            new RubixInputSplit<K, V>(job.getConfiguration(),
                                                      path,
                                                      keyData.getKey(),
                                                      keyData.getOffset(),
                                                      keyData.getLength(),
                                                      keyData.getBlockId(),
                                                      keyData.getNumRecords(),
                                                      rubixFile.getKeyClass(),
                                                      rubixFile.getValueClass(),
                                                      rubixFile.getSchema(),
                                                      rubixFile.getBlockSerializationType());

                    splits.add(split);
                }

            }
            catch (ClassNotFoundException e)
            {
                e.printStackTrace();
            }
            catch (InstantiationException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalAccessException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return splits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
                                                 TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        return new RubixRecordReader<K, V>();
    }
}
