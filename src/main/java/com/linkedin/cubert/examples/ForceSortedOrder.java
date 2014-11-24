package com.linkedin.cubert.examples;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.utils.JsonUtils;

public class ForceSortedOrder implements TupleOperator
{
    private Block block;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        return block.next();
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();

        JsonNode args = json.get("args");

        if (args == null || !args.has("sortKeys"))
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Missing 'sortKeys' parameter");

        String[] sortKeys = JsonUtils.getText(args, "sortKeys").split(",");

        return new PostCondition(condition.getSchema(),
                                 condition.getPartitionKeys(),
                                 sortKeys);
    }
}
