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

package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.plan.physical.GenerateDictionary;
import com.linkedin.cubert.utils.CodeDictionary;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * A TupleOperator that translates the coded column values back to the original value by
 * consulting the dictionary.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DictionaryDecodeOperator implements TupleOperator
{

    private CodeDictionary[] dictionaries;
    private Block dataBlock;
    private Tuple decodedTuple;
    private int numColumns;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        // Get the dictionary
        Map<String, CodeDictionary> dictionaryMap = null;
        if (json.has("path"))
        {
            // load the dictionary from file
            String dictionaryName = json.get("path").getTextValue();
            String dictionaryPath = FileCache.get(dictionaryName);
            dictionaryPath = dictionaryPath + "/part-r-00000.avro";
            dictionaryMap =
                    GenerateDictionary.loadDictionary(dictionaryPath, false, null);
        }
        else
        {
            // this is inline dictionary
            JsonNode dictionary = json.get("dictionary");

            Iterator<String> nameIterator = dictionary.getFieldNames();
            dictionaryMap = new HashMap<String, CodeDictionary>();
            while (nameIterator.hasNext())
            {
                String name = nameIterator.next();
                ArrayNode values = (ArrayNode) dictionary.get(name);
                CodeDictionary codeDictionary = new CodeDictionary();
                for (JsonNode value : values)
                {
                    codeDictionary.addKey(value.getTextValue());
                }
                dictionaryMap.put(name, codeDictionary);
            }
        }

        dataBlock = input.values().iterator().next();
        BlockSchema inputSchema = dataBlock.getProperties().getSchema();
        numColumns = inputSchema.getNumColumns();

        decodedTuple = TupleFactory.getInstance().newTuple(numColumns);

        // create dictionary array
        dictionaries = new CodeDictionary[numColumns];

        for (int i = 0; i < numColumns; i++)
        {
            String colName = inputSchema.getName(i);

            if (dictionaryMap.containsKey(colName))
            {
                dictionaries[i] = dictionaryMap.get(colName);
            }
            else
            {
                dictionaries[i] = null;
            }
        }

    }

    private BlockSchema generateSchema(Map<String, CodeDictionary> dictionaryMap,
                                       BlockSchema originalSchema)
    {
        numColumns = originalSchema.getNumColumns();

        ColumnType[] columnTypes = new ColumnType[numColumns];

        // create dictionary array
        dictionaries = new CodeDictionary[numColumns];

        decodedTuple = TupleFactory.getInstance().newTuple(numColumns);

        for (int i = 0; i < columnTypes.length; i++)
        {
            ColumnType type = new ColumnType();
            columnTypes[i] = type;

            type.setName(originalSchema.getName(i));

            if (dictionaryMap.containsKey(type.getName()))
            {
                // this column is decoded. Transform the schema
                type.setType(DataType.STRING);
                dictionaries[i] = dictionaryMap.get(type.getName());
            }
            else
            {
                // this column is not decoded. Keep the schema intact
                type.setType(originalSchema.getType(i));
                dictionaries[i] = null;
            }
        }

        return new BlockSchema(columnTypes);
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple tuple = dataBlock.next();
        if (tuple == null)
            return null;

        for (int i = 0; i < numColumns; i++)
        {
            if (dictionaries[i] == null)
            {
                // this column is not decoded
                decodedTuple.set(i, tuple.get(i));
            }
            else
            {
                // this column is decoded
                if (tuple.get(i) == null)
                {
                    decodedTuple.set(i, null);
                }
                else
                {
                    int code = ((Number) tuple.get(i)).intValue();
                    String val = dictionaries[i].getValueForCode(code);

                    if (val == null)
                        throw new RuntimeException("code '" + code
                                + "' is missing encoding in dictionary.");
                    decodedTuple.set(i, val);
                }
            }
        }

        return decodedTuple;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        String inputBlockName = JsonUtils.getText(json, "input");
        PostCondition inputCondition = preConditions.get(inputBlockName);
        BlockSchema inputSchema = inputCondition.getSchema();

        Map<String, CodeDictionary> dictionaryMap = new HashMap<String, CodeDictionary>();
        if (json.has("columns"))
        {
            String[] columns = JsonUtils.asArray(json, "columns");
            for (String column : columns)
                dictionaryMap.put(column, new CodeDictionary());
        }
        else
        {
            JsonNode dictionary = json.get("dictionary");
            // this is inline dictionary
            Iterator<String> nameIterator = dictionary.getFieldNames();
            while (nameIterator.hasNext())
            {
                String name = nameIterator.next();
                ArrayNode values = (ArrayNode) dictionary.get(name);
                CodeDictionary codeDictionary = new CodeDictionary();
                for (JsonNode value : values)
                {
                    codeDictionary.addKey(value.getTextValue());
                }
                dictionaryMap.put(name, codeDictionary);
            }
        }

        int numColumns = inputSchema.getNumColumns();

        ColumnType[] columnTypes = new ColumnType[numColumns];

        int i = 0;
        for (ColumnType ct : inputSchema.getColumnTypes())
        {
            ColumnType type = new ColumnType();
            columnTypes[i] = type;

            final String name = ct.getName();
            type.setName(name);

            if (dictionaryMap.containsKey(name))
            {
                // this column is decoded. Transform the schema
                type.setType(DataType.STRING);
            }
            else
            {
                // this column is not decoded. Keep the schema intact
                type.setType(ct.getType());
                type.setColumnSchema(ct.getColumnSchema());
            }
            i++;
        }

        BlockSchema schema = new BlockSchema(columnTypes);

        return new PostCondition(schema,
                                 inputCondition.getPartitionKeys(),
                                 inputCondition.getSortKeys());
    }

}
