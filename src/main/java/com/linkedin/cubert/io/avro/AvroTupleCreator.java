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

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.TupleCreator;

/**
 * Creates Tuple from Avro data source.
 * 
 * @author Maneesh Varshney
 * 
 */
public class AvroTupleCreator implements TupleCreator
{
    @Override
    public Tuple create(Object key, Object value)
    {
        return (Tuple) value;
    }

    @Override
    public void setup(JsonNode json)
    {

    }

}
