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

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.io.DatumReader;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.CachedFileReader;
import com.linkedin.cubert.pig.piggybank.storage.avro.PigAvroDatumReader;
import com.linkedin.cubert.utils.AvroUtils;

public class AvroFileReader implements CachedFileReader
{

    private DataFileReader<Object> dataFileReader;
    private BlockSchema schema;

    @Override
    public void open(JsonNode json, File file) throws IOException
    {
        DatumReader<Object> datumReader =
                new PigAvroDatumReader(AvroUtils.getSchema(new SeekableFileInput(file)));
        dataFileReader = new DataFileReader<Object>(file, datumReader);
        schema = AvroUtils.convertToBlockSchema(dataFileReader.getSchema());
    }

    @Override
    public Tuple next()
    {
        if (!dataFileReader.hasNext())
            return null;
        Tuple outputTuple = (Tuple) dataFileReader.next();
        return outputTuple;
    }

    @Override
    public void close() throws IOException
    {
        dataFileReader.close();
    }

    @Override
    public BlockSchema getSchema()
    {
        return schema;
    }

}
