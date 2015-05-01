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
import org.apache.avro.file.CodecFactory;
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
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.pig.piggybank.storage.avro.PigAvroDatumWriter;
import com.linkedin.cubert.utils.AvroUtils;

/**
 * Writes the TEE data into Avro format.
 * 
 * @author Maneesh Varshney
 * 
 */
public class AvroTeeWriter implements TeeWriter
{
    /** The configuration key for Avro deflate level. */
    public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";

    /** The default deflate level. */
    public static final int DEFAULT_DEFLATE_LEVEL = 1;

    /** The configuration key for the Avro codec. */
    public static final String OUTPUT_CODEC = "avro.output.codec";

    /** The deflate codec */
    public static final String DEFLATE_CODEC = "deflate";

    private DataFileWriter<Object> dataFileWriter;

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

        GenericDatumWriter<Object> datumWriter =
                new PigAvroDatumWriter(avroSchema);
        dataFileWriter = new DataFileWriter<Object>(datumWriter);

        // if compression is requested, set the proper compression codec
        if (PhaseContext.getConf().getBoolean("mapred.output.compress", false))
        {
            int level = conf.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            String codecName = conf.get(OUTPUT_CODEC, DEFLATE_CODEC);
            CodecFactory factory =
                    codecName.equals(DEFLATE_CODEC) ? CodecFactory.deflateCodec(level)
                            : CodecFactory.fromString(codecName);
            dataFileWriter.setCodec(factory);
        }

        dataFileWriter.create(avroSchema, fs.create(teePath));
    }

    @Override
    public void write(Tuple tuple) throws IOException
    {
        dataFileWriter.append(tuple);
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
