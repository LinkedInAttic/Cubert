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

package com.linkedin.cubert.operator.aggregate;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.utils.JsonUtils;

public class SumAggregation extends AbstractAggregationFunction
{
    @Override
    public void resetState()
    {
        longAggVal = 0;
        doubleAggVal = 0;
    }

    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
        Object obj = inputTuple.get(inputColumnIndex);
        if (obj == null)
            return;

        nonNullValueSeen = true;

        Number val = (Number) inputTuple.get(inputColumnIndex);

        if (inputDataType.isReal())
            doubleAggVal += val.doubleValue();
        else
            longAggVal += val.longValue();
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException
    {
        String[] inputColNames = JsonUtils.asArray(json, "input");
        if (inputColNames.length != 1)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Only one column expected for SUM. Found: "
                                                    + JsonUtils.get(json, "input"));

        String inputColName = inputColNames[0];
        if (!inputSchema.hasIndex(inputColName))
            throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                            inputColName);

        String outputColName = JsonUtils.getText(json, "output");
        DataType inputType = inputSchema.getType(inputSchema.getIndex(inputColName));

        return new BlockSchema(inputType + " " + outputColName);
    }
}
