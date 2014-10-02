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

package com.linkedin.cubert.functions;

import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * Retrieves a value from the map given a key.
 * <p>
 * Note: this function currently only works when the map values are strings.
 * 
 * @author Maneesh Varshney
 * 
 */
public class MapProjection extends Function
{
    private final String key;

    public MapProjection(String key)
    {
        this.key = key;
    }

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) tuple.get(0);
        return map.get(key);
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        return new ColumnType("", DataType.STRING);
    }

}
