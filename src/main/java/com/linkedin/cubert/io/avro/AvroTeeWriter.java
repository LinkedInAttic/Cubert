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

package com.linkedin.cubert.io.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.utils.AvroUtils;

/**
 * Writes the TEE data into Avro format.
 * 
 * @author Maneesh Varshney
 * 
 */
public class AvroTeeWriter implements TeeWriter
{
    private DataFileWriter<GenericRecord> dataFileWriter;
    private Record record;
    private int numColumns;

    @Override
    public void open(Configuration conf,
                     JsonNode json,
                     BlockSchema schema,
                     Path root,
                     String filename) throws IOException
    {
        Path teePath = new Path(root, filename + ".avro");
        FileSystem fs = FileSystem.get(conf);

        Schema avroSchema = AvroUtils.convertFromBlockSchema("record", schema);
        record = new Record(avroSchema);
        numColumns = schema.getNumColumns();

        DatumWriter<GenericRecord> datumWriter =
                new GenericDatumWriter<GenericRecord>(avroSchema);
        dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(avroSchema, fs.create(teePath));
    }

    @Override
    public void write(Tuple tuple) throws IOException
    {
        for (int i = 0; i < numColumns; i++)
        {
            Object field = tuple.get(i);
            record.put(i, field);
        }
        dataFileWriter.append(record);
    }

    @Override
    public void close() throws IOException
    {
        dataFileWriter.close();
    }

    @Override
    public void flush() throws IOException
    {
        dataFileWriter.flush();
    }

}
