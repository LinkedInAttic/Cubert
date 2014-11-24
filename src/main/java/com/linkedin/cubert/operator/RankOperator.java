package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Tuple operator that will generate a rank/ordering for each tuple. Analytical Function
 * Operator: works on a sorted block generates rank for a "partition by" (columns)
 * sub-block
 *
 * @author Mani Parkhe
 */
public class RankOperator implements TupleOperator
{
    private BlockSchema outputSchema = null;
    private Tuple outputTuple;
    private int rankColumnIndex;
    private long rank = Long.MIN_VALUE;

    private Block inputBlock = null;
    private Block currentBlock;
    private PivotBlockOperator pivotBlockOp;

    /**
     * {@inheritDoc}
     *
     * @see com.linkedin.rcf.operator.TupleOperator#setInput(java.util.Map,
     *      org.codehaus.jackson.JsonNode)
     */
    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
    InterruptedException
    {
        inputBlock = input.values().iterator().next();
        BlockSchema inputSchema = inputBlock.getProperties().getSchema();

        outputSchema = getOutputSchema(json, inputSchema);
        outputTuple = TupleFactory.getInstance().newTuple(outputSchema.getNumColumns());
        rankColumnIndex = outputSchema.getNumColumns() - 1;

        pivotBlockOp = new PivotBlockOperator();
        String[] groupByColumns = JsonUtils.asArray(JsonUtils.get(json, "groupBy"));
        pivotBlockOp.setInput(inputBlock, groupByColumns, false);

        resetState();
    }

    /**
     * Create output schema : input schema + rank (column name user specified)
     *
     * @param json
     */
    private BlockSchema getOutputSchema(JsonNode json, BlockSchema inputSchema)
    {
        String rankColumnType = "long " + JsonUtils.getText(json, "rankAs");
        return inputSchema.append(new BlockSchema(rankColumnType));
    }

    /**
     * This is the meat of the operator. While current block as tuples, increment rank and
     * output tuple. When current block runs out -- get new pivot block. Since
     * preconditions dictate that the block is partitioned by GROUP BY keys (or subset)
     * and sorted by a combination of GROUP BY + ORDER BY keys there is no need to further
     * compare tuples.
     *
     * @see com.linkedin.rcf.operator.TupleOperator#next()
     */
    @Override
    public Tuple next() throws IOException,
    InterruptedException
    {
        if (currentBlock == null)
        {
            if (!nextBlock())
                return null;
        }

        // Advance pivot if this block is exhausted or top_n have been exhausted.
        Tuple tuple = currentBlock.next();
        if (tuple == null)
        {
            resetState();
            return this.next();
        }

        // Valid tuple: increment rank and check
        rank++;
        if (rank < 0)
            throw new RuntimeException("Rank overflow!");

        for (int i = 0; i < rankColumnIndex; i++)
            outputTuple.set(i, tuple.get(i));

        outputTuple.set(rankColumnIndex, rank);
        return outputTuple;
    }

    /*
     * Get next pivot block.
     */
    private boolean nextBlock() throws IOException,
    InterruptedException
    {
        // Get next pivot block
        currentBlock = pivotBlockOp.next();

        if (null == currentBlock)
            return false;

        return true;
    }

    private void resetState()
    {
        rank = 0; // Starting rank==1. 'rank' is pre-incremented before set.
        currentBlock = null;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.linkedin.rcf.operator.TupleOperator#getPostCondition(java.util.Map,
     *      org.codehaus.jackson.JsonNode)
     */
    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
                                          {
        String inputBlock = JsonUtils.getText(json, "input");
        PostCondition inputBlockPostCondition = preConditions.get(inputBlock);
        BlockSchema inputSchema = inputBlockPostCondition.getSchema();

        String[] groupByKeys = JsonUtils.asArray(JsonUtils.get(json, "groupBy"));
        String[] orderByKeys = JsonUtils.asArray(JsonUtils.get(json, "orderBy"));

        String[] expectedSortOrder = CommonUtils.concat(groupByKeys, orderByKeys);

        // Check: all orderBy and groupBy columns exist in inputBlock's schema
        for (String colName : expectedSortOrder)
        {
            if (!inputSchema.hasIndex(colName))
            {
                String msg =
                        String.format("Input block '%s' is missing expected column '%s'",
                                      inputBlock,
                                      colName);
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                msg);
            }
        }

        // Check: partition order
        // Logic -- partition keys should match group-by keys or should prefix group-by
        // keys.
        String[] partitionKeys = inputBlockPostCondition.getPartitionKeys();
        if (groupByKeys.length > 0
                && (partitionKeys == null || partitionKeys.length == 0 || !CommonUtils.isPrefix(groupByKeys,
                                                                                                partitionKeys)))
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS,
                                            String.format("Found=%s, Expected=%s",
                                                          partitionKeys == null
                                                          ? "[null]"
                                                                  : Arrays.toString(partitionKeys),
                                                                  groupByKeys == null
                                                                  ? "[null]"
                                                                          : Arrays.toString(groupByKeys)));
        }

        // Check: sorting order
        String[] sortKeys = inputBlockPostCondition.getSortKeys();
        if (expectedSortOrder.length > 0
                && !CommonUtils.isPrefix(sortKeys, expectedSortOrder))
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                            String.format("Found=%s, Expected=%s",
                                                          sortKeys == null
                                                          ? "[null]"
                                                                  : Arrays.toString(sortKeys),
                                                                  expectedSortOrder == null
                                                                  ? "[null]"
                                                                          : Arrays.toString(expectedSortOrder)));
        }

        BlockSchema schema = getOutputSchema(json, inputSchema);
        return new PostCondition(schema, partitionKeys, sortKeys);
                                          }
}
