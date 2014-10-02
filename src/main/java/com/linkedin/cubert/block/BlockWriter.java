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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

/**
 * An object that writes a {@code Block}.
 * 
 * This object is used to write the block both at the mapper and the reducer.
 * 
 * @author Maneesh Varshney
 * 
 */
public interface BlockWriter
{
    void configure(JsonNode json) throws JsonParseException,
            JsonMappingException,
            IOException;

    void write(Block block, CommonContext context) throws IOException,
            InterruptedException;
}
