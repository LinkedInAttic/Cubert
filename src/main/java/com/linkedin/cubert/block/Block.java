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

package com.linkedin.cubert.block;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

/**
 * A block of Tuples of a dataset.
 * 
 * Each block is uniquely identified by a blockId and partitionKey. Each block is aware of
 * the schema of the columns in the Tuples.
 * 
 * The rows can be retrieved via the {@code next} method. Note that object returned by
 * this call may be reused for subsequent call. If the tuple must be retained, the called
 * must make a copy via the {@code Tuple.clone()} method.
 * 
 * @author Maneesh Varshney
 * 
 */
public interface Block
{
    void configure(JsonNode json) throws IOException,
            InterruptedException;

    BlockProperties getProperties();

    Tuple next() throws IOException,
            InterruptedException;

    void rewind() throws IOException;

}
