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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.utils.SchemaUtils;

/**
 * Wraps Pig's {@code EvalFunc} as a {@code Function}.
 * <p>
 * Note: only the getCacheFiles, outputSchema and eval methods from the Pig's EvalFunc are
 * currently used.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PigEvalFuncWrapper extends Function
{
    private final EvalFunc<Object> func;

    public PigEvalFuncWrapper(EvalFunc<Object> func)
    {
        this.func = func;
    }

    @Override
    public Object eval(Tuple tuple) throws IOException
    {
        return func.exec(tuple);
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        ColumnType outputType;
        try
        {
            Schema pigSchema = SchemaUtils.convertFromBlockSchema(inputSchema);
            Schema pigOutputSchema = func.outputSchema(pigSchema);

            if (pigOutputSchema != null)
            {

                BlockSchema outputSchema =
                        SchemaUtils.convertToBlockSchema(pigOutputSchema);
                if (outputSchema.getNumColumns() > 1)
                {
                    outputType = new ColumnType(null, DataType.TUPLE);
                    outputType.setColumnSchema(outputSchema);
                }
                else
                {
                    outputType = new ColumnType(null, outputSchema.getType(0));
                }

                return outputType;
            }
            else
            {
                Type returnType = func.getReturnType();
                byte pigType = org.apache.pig.data.DataType.findType(returnType);
                String pigTypeName = org.apache.pig.data.DataType.findTypeName(pigType);
                String dataTypeStr = SchemaUtils.convertoRCFTypeName(pigTypeName);
                DataType dataType = DataType.valueOf(dataTypeStr.toUpperCase());

                return new ColumnType(null, dataType);
            }
        }
        catch (FrontendException e)
        {
            throw new PreconditionException(PreconditionExceptionType.MISC_ERROR,
                                            e.getDetailedMessage());
        }
    }

    @Override
    public List<String> getCacheFiles()
    {
        return func.getCacheFiles();
    }

}
