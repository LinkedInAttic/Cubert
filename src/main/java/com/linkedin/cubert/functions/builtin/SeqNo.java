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

package com.linkedin.cubert.functions.builtin;

import java.io.IOException;

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * Built in SeqNo function.
 * 
 * This function generates a new sequence number per invocation. The sequence number
 * counter is static, that is, multiple objects of this function will not have overlapping
 * sequence numbers.
 * 
 * @author Maneesh Varshney
 * 
 */
public class SeqNo extends Function
{
    private static long seqno = 1;

    @Override
    public Object eval(Tuple tuple) throws IOException
    {
        return seqno++;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        return new ColumnType(null, DataType.LONG);
    }

}
