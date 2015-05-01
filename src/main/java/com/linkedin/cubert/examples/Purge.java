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
package com.linkedin.cubert.examples;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.NeedCachedFiles;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Given a file with members to purge, the columnName and the source file, purge the
 * members.
 */

public class Purge implements TupleOperator, NeedCachedFiles
{
    private Block block;
    private Tuple output;
    private long numRecords;
    private long remainingRecords;
    private long recordsPurged;
    private final Set<Integer> membersToPurge = new HashSet<Integer>();
    private String columnName;
    private Configuration conf;
    private String tempFileName;
    private String purgeFileName;
    private List<String> filesToCache = new ArrayList<String>();

    public Purge(String purgeListFileName)
    {
        filesToCache.add(purgeListFileName + "#purgeFileName");
    }

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();
        conf = PhaseContext.getConf();
        output = TupleFactory.getInstance().newTuple(3);
        purgeFileName = FileCache.get(filesToCache.get(0));

        if (purgeFileName == null)
        {
            throw new IOException("purgeFileName is null");
        }

        loadMembersToPurge(purgeFileName);

        String columnName = JsonUtils.getText(json.get("args"), "purgeColumnName");
        setColumnName(columnName);

        // Create temp file
        Path root = null;
        String filename = null;
        tempFileName = null;

        if (PhaseContext.isMapper())
        {
            root = FileOutputFormat.getWorkOutputPath(PhaseContext.getMapContext());
            filename =
                    FileOutputFormat.getUniqueFile(PhaseContext.getMapContext(),
                                                   "tempFileForPurge",
                                                   "");
        }
        else
        {
            root = FileOutputFormat.getWorkOutputPath(PhaseContext.getRedContext());
            filename =
                    FileOutputFormat.getUniqueFile(PhaseContext.getRedContext(),
                                                   "tempFileForPurge",
                                                   "");
        }

        tempFileName = root + "/" + filename;
    }

    private void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    private DataFileReader<GenericRecord> createDataFileReader(String filename,
                                                               boolean localFS) throws IOException
    {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader;

        if (localFS)
        {
            dataFileReader =
                    new DataFileReader<GenericRecord>(new File(filename), datumReader);
        }
        else
        {
            Path path = new Path(filename);
            SeekableInput input = new FsInput(path, conf);
            dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);
        }

        return dataFileReader;
    }

    private DataFileWriter<GenericRecord> createDataFileWriter(DataFileReader<GenericRecord> dataFileReader) throws IllegalArgumentException,
            IOException
    {
        Schema schema = dataFileReader.getSchema();
        DatumWriter<GenericRecord> datumWriter =
                new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(datumWriter);

        // Get the codec of the reader
        String codecStr = dataFileReader.getMetaString(DataFileConstants.CODEC);
        int level = conf.getInt("avro.mapred.deflate.level", 1);
        String codecName = conf.get("avro.output.codec", codecStr);
        CodecFactory factory =
                codecName.equals("deflate") ? CodecFactory.deflateCodec(level)
                        : CodecFactory.fromString(codecName);

        // Set the codec of the writer
        writer.setCodec(factory);

        writer.setSyncInterval(conf.getInt("avro.mapred.sync.interval",
                                           Math.max(conf.getInt("io.file.buffer.size",
                                                                16000), 16000)));

        writer.create(schema,
                      new Path(tempFileName).getFileSystem(conf)
                                            .create(new Path(tempFileName)));
        return writer;
    }

    private void loadMembersToPurge(String filename) throws IOException
    {
        // TODO: "memberId" column name should be configurable
        DataFileReader<GenericRecord> dataFileReader =
                createDataFileReader(filename, true);
        while (dataFileReader.hasNext())
        {
            GenericRecord record = dataFileReader.next();
            Integer memberId = (Integer) record.get("memberId");
            if (memberId == null)
            {
                throw new NullPointerException("memberId is null");
            }
            membersToPurge.add(((Number) record.get("memberId")).intValue());
        }
        dataFileReader.close();
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple t = block.next();

        if (t == null)
            return null;

        // get the file path to process
        String filename = (String) t.get(0);

        // process the file (purge the members) and write to temp file
        purge(filename, tempFileName);

        // move the temp file to the original path
        swap(filename, tempFileName);

        // set the fields for the output tuple
        output.set(0, filename);
        output.set(1, numRecords);
        output.set(2, recordsPurged);

        // increment hadoop counter to let know that we have processed one more file
        PhaseContext.getCounter("Purge", "Files processed").increment(1);
        return output;
    }

    private void purge(String src, String dst) throws IOException
    {
        DataFileReader<GenericRecord> dataFileReader = createDataFileReader(src, false);
        DataFileWriter<GenericRecord> writer = createDataFileWriter(dataFileReader);

        numRecords = 0;
        recordsPurged = 0;
        remainingRecords = 0;

        // Copy
        while (dataFileReader.hasNext())
        {
            numRecords++;
            GenericRecord record = dataFileReader.next();
            if (record == null)
            {
                continue;
            }

            Number column = (Number) record.get(columnName);
            if ((column == null) || (!membersToPurge.contains(column.intValue())))
            {
                remainingRecords++;
                writer.append(record);
            }
        }

        recordsPurged = numRecords - remainingRecords;
        writer.close();
        dataFileReader.close();
    }

    private void swap(String original, String temp) throws IOException
    {
        Path source = new Path(temp);
        Path dest = new Path(original);
        FileSystem fs = dest.getFileSystem(conf);

        fs.delete(dest, true);
        fs.rename(source, dest);
    }

    @Override
    public List<String> getCachedFiles()
    {
        return filesToCache;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        BlockSchema schema =
                new BlockSchema("String filename, LONG records, LONG recordsPurged");
        JsonNode args = json.get("args");

        if (args == null || !args.has("purgeColumnName"))
            throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                            "Missing 'purgeColumnName' parameter");

        return new PostCondition(schema, null, null);
    }
}
