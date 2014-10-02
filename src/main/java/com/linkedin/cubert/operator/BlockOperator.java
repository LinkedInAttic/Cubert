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

package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;

/**
 * Operator that generates blocks.
 * 
 * @author Maneesh Varshney
 * 
 */
public interface BlockOperator
{

    void setInput(Configuration conf, Map<String, Block> input, JsonNode json) throws IOException,
            InterruptedException;

    Block next() throws IOException,
            InterruptedException;
}
