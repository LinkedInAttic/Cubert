package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.plan.physical.GenerateDictionary;
import com.linkedin.cubert.utils.CodeDictionary;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Reduce side Refersh Dictionary Operator. Will take in shuffled data from map side
 * operator. For each new {column_name, column_value} received, assign new dictionary
 * code.
 * 
 * @author Mani Parkhe
 */
public class DictionaryRefreshReduceSideOperator implements TupleOperator
{
    private Block block;
    private Map<String, CodeDictionary> dictionaryMap = null;
    private boolean dictionaryUpdated = false;

    private Tuple output;
    private Integer newCode;
    private Iterator<Entry<String, CodeDictionary>> dictionaryIterator = null;
    private Iterator<String> valuesAndCodesIterator = null;
    private String currentColumnName;
    private CodeDictionary currentDictionary;

    /**
     * {@inheritDoc}
     * 
     * @see com.linkedin.cubert.operator.TupleOperator#setInput(java.util.Map,
     *      org.codehaus.jackson.JsonNode, com.linkedin.cubert.block.BlockProperties)
     */
    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        if (json.has("dictionary"))
        {
            // load the dictionary from file
            String dictionaryName = JsonUtils.getText(json, "dictionary");
            String cachedPath = FileCache.get(dictionaryName);
            dictionaryMap = GenerateDictionary.loadDictionary(cachedPath, false, null);
        }
        else
        {
            dictionaryMap = new HashMap<String, CodeDictionary>();

            String[] columns = JsonUtils.getText(json, "columns").split(",");
            for (String name : columns)
                dictionaryMap.put(name, new CodeDictionary());
        }

        output = TupleFactory.getInstance().newTuple(3);
        String inputBlockName = JsonUtils.asArray(json, "input")[0];
        block = input.get(inputBlockName);
        dictionaryUpdated = false;
    }

    /**
     * Fetch the next dictionary to report.
     * 
     * @return <code> true </code> if there are more dictionaries to report.
     *         <code> false </code> otherwise.
     */
    private boolean fetchNextDictionary()
    {
        if (!dictionaryIterator.hasNext())
            return false;

        Entry<String, CodeDictionary> entry = dictionaryIterator.next();
        currentColumnName = entry.getKey();
        currentDictionary = entry.getValue();

        valuesAndCodesIterator = currentDictionary.keySet().iterator();
        return true;
    }

    /**
     * {@inheritDoc} First update dictionary with new values from input block. Output 1
     * row per dictionary column for each distinct value.
     * 
     * @see com.linkedin.cubert.operator.TupleOperator#next()
     */
    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        // Update dictionary with new values from input block
        if (!dictionaryUpdated)
        {
            updateDictionaries();
            dictionaryUpdated = true;
            dictionaryIterator = dictionaryMap.entrySet().iterator();
        }

        // Fetch the (first or) next dictionary column.
        if (valuesAndCodesIterator == null)
        {
            if (!fetchNextDictionary())
                return null;
        }

        // Exhausted current dictionary, get the next one.
        if (!valuesAndCodesIterator.hasNext())
        {
            valuesAndCodesIterator = null;
            return this.next();
        }

        // For current (dictionary) column, report next distinct column value and
        // dictionary code.
        String columnValue = valuesAndCodesIterator.next();
        newCode = currentDictionary.getCodeForKey(columnValue);

        output.set(0, currentColumnName);
        output.set(1, columnValue);
        output.set(2, newCode);

        return output;
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecException
     *             Iterate through all tuples of input block and update dictionary for
     *             missing column values.
     */
    private void updateDictionaries() throws IOException,
            InterruptedException,
            ExecException
    {
        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            String colName = (String) tuple.get(0);
            String colValue = (String) tuple.get(1);

            CodeDictionary dictionary = dictionaryMap.get(colName);
            if (dictionary.getCodeForKey(colValue) == -1)
                dictionary.addKey(colValue);
        }

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.linkedin.cubert.operator.TupleOperator#getPostCondition(java.util.Map,
     *      org.codehaus.jackson.JsonNode)
     */
    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        return new PostCondition(new BlockSchema("STRING colname, STRING colvalue, INT code"),
                                 null,
                                 null);
    }
}
