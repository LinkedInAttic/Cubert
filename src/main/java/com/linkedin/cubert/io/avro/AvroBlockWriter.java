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

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.CommonContext;

/**
 * Writes a block in AvroKey format (the tuples are stored as keys, and the values are
 * null).
 * 
 * @author Maneesh Varshney
 * 
 */
public class AvroBlockWriter implements BlockWriter
{
    @Override
    public void configure(JsonNode json) throws IOException
    {

    }

    @Override
    public void write(Block block, CommonContext context) throws IOException,
            InterruptedException
    {
        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            context.write(null, tuple);
        }
    }
}
