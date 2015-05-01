/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */
package com.linkedin.cubert.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.linkedin.cubert.utils.*;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.io.avro.PigAvroInputFormatAdaptor;
import com.linkedin.cubert.io.text.PigTextOutputFormatWrapper;
import com.linkedin.cubert.io.rubix.RubixInputFormat;
import com.linkedin.cubert.io.rubix.RubixOutputFormat;
import com.linkedin.cubert.io.SerializerUtils;

public class CubertMD
{

    public static void execCommand(String[] cmdArgs) throws IOException
    {
        execCommand(cmdArgs, false);
    }

    public static void execCommand(String[] commandArgs, boolean interactive) throws IOException
    {
        if (commandArgs[0].equalsIgnoreCase("READ"))
        {
            Map<String, String> entries = execMetafileRead(commandArgs[1]);
            if (interactive)
            {
                for (String ek : entries.keySet())
                    System.out.println(String.format("Key=%s Value=%s",
                                                     ek,
                                                     entries.get(ek)));
            }
        }
        else if (commandArgs[0].equalsIgnoreCase("WRITE")
                || commandArgs[0].equalsIgnoreCase("UPDATE"))
            execMetafileUpdate(commandArgs[1],
                               (String[]) Arrays.copyOfRange(commandArgs,
                                                             2,
                                                             commandArgs.length));
    }

    public static void execMetafileUpdate(String metaFilePath, String[] keyValues) throws IOException
    {
        HashMap<String, String> metaFileEntries = readMetafile(metaFilePath);
        for (int i = 0; i < keyValues.length / 2; i++)
        {
            metaFileEntries.put(keyValues[2 * i], keyValues[2 * i + 1]);
        }

        writeMetaFile(metaFilePath, metaFileEntries);
    }

    private static void writeMetaFile(String metaFilePath,
                                      HashMap<String, String> metaFileKeyValues) throws IOException
    {
        Job tempjob = new Job();
        Configuration tempconf = tempjob.getConfiguration();
        FileSystem fs = FileSystem.get(tempconf);

        FSDataOutputStream outStream = fs.create(new Path(metaFilePath + "/.meta"));
        for (String key : metaFileKeyValues.keySet())
            outStream.write((key + " " + metaFileKeyValues.get(key) + "\n").getBytes());
        outStream.flush();
        outStream.close();
    }

    public static Map<String, String> execMetafileRead(String metaFilePath) throws IOException
    {
        HashMap<String, String> metaFileEntries = readMetafile(metaFilePath);
        return metaFileEntries;
    }

    public static HashMap<String, String> readMetafile(String metaFilePath) throws IOException
    {
        Job tempjob = new Job();
        Configuration tempconf = tempjob.getConfiguration();
        FileSystem fs = FileSystem.get(tempconf);

        HashMap<String, String> result = new HashMap<String, String>();
        FSDataInputStream inStream;
        try
        {
            inStream = fs.open(new Path(metaFilePath + "/.meta"));

            BufferedReader breader = new BufferedReader(new InputStreamReader(inStream));
            String line;
            while ((line = breader.readLine()) != null)
            {
                String[] splits = line.split("\\s+");
                result.put(splits[0], splits[1]);
            }
        }
        catch (IOException e)
        {
            return result;
        }
        return result;
    }

    public static void main(String[] args) throws IOException
    {
        execCommand(args, true);
    }

}
