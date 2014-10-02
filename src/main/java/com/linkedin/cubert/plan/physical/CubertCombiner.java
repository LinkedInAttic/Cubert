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

package com.linkedin.cubert.plan.physical;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.CommonContext;
import com.linkedin.cubert.block.ContextBlock;
import com.linkedin.cubert.operator.OperatorFactory;
import com.linkedin.cubert.operator.OperatorType;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.operator.aggregate.AggregationFunction;
import com.linkedin.cubert.operator.aggregate.AggregationFunctions;
import com.linkedin.cubert.operator.aggregate.AggregationType;
import com.linkedin.cubert.plan.physical.CubertReducer.ReduceContext;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

/**
 * Combines data at the mapper during the shuffle stage.
 * 
 * This object is instantiated if the job configuration has "shuffle" property, and
 * further, if the shuffle property as "aggregations" property.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CubertCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple>
{
    @Override
    public void run(Context context) throws IOException,
            InterruptedException
    {
        Configuration conf = context.getConfiguration();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode shuffleJson =
                mapper.readValue(conf.get(CubertStrings.JSON_SHUFFLE), JsonNode.class);

        ObjectNode groupByJson = mapper.createObjectNode();
        groupByJson.put("name", shuffleJson.get("name"));
        groupByJson.put("type", shuffleJson.get("type"));
        groupByJson.put("groupBy", shuffleJson.get("pivotKeys"));
        groupByJson.put("aggregates", shuffleJson.get("aggregates"));

        String[] keyColumns = JsonUtils.asArray(shuffleJson, "pivotKeys");
        BlockSchema fullSchema = new BlockSchema(shuffleJson.get("schema"));
        BlockSchema valueSchema = fullSchema.getComplementSubset(keyColumns);

        CommonContext commonContext = new ReduceContext(context);
        Block input = new ContextBlock(commonContext);
        input.configure(shuffleJson);

        try
        {
            TupleOperator operator =
                    OperatorFactory.getTupleOperator(OperatorType.GROUP_BY);

            Map<String, Block> inputMap = new HashMap<String, Block>();
            inputMap.put("groupbyBlock", input);
            BlockProperties props =
                    new BlockProperties(null, fullSchema, (BlockProperties) null);
            operator.setInput(inputMap, groupByJson, props);

            String[] valueColumns = valueSchema.getColumnNames();
            Tuple tuple;
            Tuple key = TupleFactory.getInstance().newTuple(keyColumns.length);
            Tuple value = TupleFactory.getInstance().newTuple(valueColumns.length);

            while ((tuple = operator.next()) != null)
            {
                TupleUtils.extractTupleWithReuse(tuple, fullSchema, key, keyColumns);
                TupleUtils.extractTupleWithReuse(tuple, fullSchema, value, valueColumns);
                context.write(key, value);
            }
        }
        // catch this exception here and don't let it propagate to hadoop; if it does,
        // there is a bug in the hadoop code which just hangs the job without killing it.
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void checkPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();

        String[] keyColumns = JsonUtils.asArray(json, "pivotKeys");
        BlockSchema outputSchema = inputSchema.getSubset(keyColumns);

        if (json.has("aggregates"))
        {
            for (JsonNode aggregateJson : json.path("aggregates"))
            {
                AggregationType aggType =
                        AggregationType.valueOf(JsonUtils.getText(aggregateJson, "type"));
                AggregationFunction aggregator = null;
                aggregator = AggregationFunctions.get(aggType, aggregateJson);
                BlockSchema aggSchema =
                        aggregator.outputSchema(inputSchema, aggregateJson);
                outputSchema = outputSchema.append(aggSchema);
            }
        }

        if (!inputSchema.equals(outputSchema))
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                            "The input and output schema for SHUFFLE must be identical."
                                                    + "\n\tInput Schema: " + inputSchema
                                                    + "\n\tOutputSchema: " + outputSchema);
    }
}
