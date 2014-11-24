package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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
import com.linkedin.cubert.utils.Pair;

/**
 * Map side operator for Refresh dictionary MR style job. This will load existing
 * dictionary in memory. For every column that needs encoding, emit column values that are
 * not already in dictionary.
 * 
 * @author Mani Parkhe
 */
public class DictionaryRefreshMapSideOperator implements TupleOperator
{

    private int numColumns;
    private CodeDictionary[] dictionaries;
    private Block dataBlock;
    private boolean[] isDictionaryField;

    Tuple output;
    private List<Set<String>> emitCache;
    Queue<Pair<String, String>> emitKeys = null;
    private String[] columnNames;
    private String replaceNull;

    // private String defaultValue;

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
        Map<String, CodeDictionary> dictionaryMap = null;

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

        // if (json.has("defaultValue"))
        // {
        // defaultValue = JsonUtils.getText(json, "defaultValue");
        // }

        String inputBlockName = JsonUtils.asArray(json, "input")[0];
        dataBlock = input.get(inputBlockName);
        BlockSchema inputSchema = dataBlock.getProperties().getSchema();
        columnNames = inputSchema.getColumnNames();
        numColumns = inputSchema.getNumColumns();

        output = TupleFactory.getInstance().newTuple(2);
        emitKeys = new LinkedList<Pair<String, String>>();

        // create dictionary array
        dictionaries = new CodeDictionary[numColumns];

        emitCache = new ArrayList<Set<String>>();
        for (int i = 0; i < numColumns; i++)
            emitCache.add(new HashSet<String>());

        isDictionaryField = new boolean[numColumns];

        for (int i = 0; i < numColumns; i++)
        {
            String colName = inputSchema.getName(i);

            boolean dict = isDictionaryField[i] = dictionaryMap.containsKey(colName);

            if (dict)
                dictionaries[i] = dictionaryMap.get(colName);
            else
                dictionaries[i] = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.linkedin.cubert.operator.TupleOperator#next()
     */
    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        while (emitKeys.peek() == null)
        {
            Tuple tuple = dataBlock.next();
            if (tuple == null)
                return null;

            processTuple(tuple);
        }

        Pair<String, String> elem = emitKeys.poll();
        output.set(0, elem.getFirst());
        output.set(1, elem.getSecond());

        return output;
    }

    /**
     * @param tuple
     *            Iterates through each element in <code> tuple </code> and populates
     *            queue for any new element seen.
     * @throws ExecException
     */
    private void processTuple(Tuple tuple) throws ExecException
    {
        for (int i = 0; i < isDictionaryField.length; i++)
        {
            if (!isDictionaryField[i])
                continue;

            Object val = tuple.get(i);
            String colValue = (val == null) ? replaceNull : val.toString();

            if (dictionaries[i].getCodeForKey(colValue) != -1
                    || emitCache.get(i).contains(colValue))
                continue;

            emitCache.get(i).add(colValue);
            emitKeys.add(new Pair<String, String>(columnNames[i], colValue));
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
        return new PostCondition(new BlockSchema("STRING colname, STRING colvalue"),
                                 null,
                                 null);
    }
}
