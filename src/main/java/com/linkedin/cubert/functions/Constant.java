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

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * Builtin function that returns a constant value.
 * 
 * This function is initialized (via constructor) with the constant value. The input tuple
 * in the eval() method is always null.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Constant extends Function
{
    private final Object constant;

    public Constant(Object constant)
    {
        this.constant = constant;
    }

    @Override
    public Object eval(Tuple tuple)
    {
        return constant;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {

        DataType type = DataType.getDataType(constant);
        return new ColumnType(null, type);
    }

}
