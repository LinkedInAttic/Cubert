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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;

/**
 * Interface for TEE operator writers.
 * 
 * This interface is different from Hadoop's OutputFormat and RecordWriter, since the
 * latter are hardcoded to select the output files and have dependency on Job object
 * (which is not available in the TEE operator).
 * 
 * @author Maneesh Varshney
 * 
 */
public interface TeeWriter
{
    /**
     * Open a file for writing TEE data.
     * 
     * @param conf
     *            the configuration object
     * @param json
     *            the JsonNode object of the TEE operator
     * @param schema
     *            the output schema
     * @param root
     *            the root folder
     * @param filename
     *            the filename without an extension. Specific TeeWriters may add extension
     *            to the filename.
     * @throws IOException
     */
    void open(Configuration conf,
              JsonNode json,
              BlockSchema schema,
              Path root,
              String filename) throws IOException;

    /**
     * Write a tuple to the TEE file.
     * 
     * @param tuple
     * @throws IOException
     */
    void write(Tuple tuple) throws IOException;

    /**
     * Flushes the underlying output stream.
     * 
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close the Tee file.
     * 
     * @throws IOException
     */
    void close() throws IOException;
}
