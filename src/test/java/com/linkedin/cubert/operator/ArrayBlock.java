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

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;

public class ArrayBlock implements Block
{

    private Iterator<Object[]> iterator;
    private final Tuple tuple;
    private final List<Object[]> rows;
    private final int blockId;
    private final BlockSchema schema;

    public ArrayBlock(List<Object[]> rows, String[] colNames)
    {
        this(rows, colNames, -1);
    }

    public ArrayBlock(List<Object[]> rows, BlockSchema schema, int blockId)
    {
        this.rows = rows;
        this.blockId = blockId;
        iterator = rows.iterator();
        this.schema = schema;
        tuple = TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }

    public ArrayBlock(List<Object[]> rows, String[] colNames, int blockId)
    {
        this.rows = rows;
        this.blockId = blockId;
        iterator = rows.iterator();

        ColumnType[] columnTypes = new ColumnType[colNames.length];
        for (int i = 0; i < colNames.length; i++)
        {
            ColumnType type = new ColumnType();
            type.setName(colNames[i]);
            if (rows.size() == 0)
            {
                type.setType("int");
            }
            else
            {
                Object element = rows.get(0)[i];
                if (element == null)
                    type.setType("long"); // backward compatibility for "null-okay" unit tests
                else if (element instanceof Integer)
                    type.setType("int");
                else if (element instanceof Long)
                    type.setType("long");
                else if (element instanceof Float)
                    type.setType("float");
                else if (element instanceof Double)
                    type.setType("double");
                else
                    throw new RuntimeException("Undefined type ");

//                switch (element.getClass().getSimpleName())
//                {
//                    case "Integer" :
//                    {
//                        type.setType("int");
//                        break;
//                    }
//                    case "Long" :
//                    {
//                        type.setType("long");
//                        break;
//                    }
//                    case "Float" :
//                    {
//                        type.setType("float");
//                        break;
//                    }
//                    case "Double" :
//                    {
//                        type.setType("double");
//                        break;
//                    }
//                    default : throw new RuntimeException("Undefined type ");
//                }
            }
            columnTypes[i] = type;
        }

        schema = new BlockSchema(columnTypes);

        tuple = TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }

    @Override
    public void configure(JsonNode json)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (!iterator.hasNext())
            return null;

        Object[] row = iterator.next();
        int ncols = row.length;
        for (int i = 0; i < ncols; i++)
        {
            tuple.set(i, row[i]);
        }
        return tuple;
    }

    @Override
    public void rewind() throws IOException
    {
        iterator = rows.iterator();
    }

    public static void assertData(Block cube, Object[][] expected, String[] colNames) throws IOException,
            InterruptedException
    {
        Tuple tuple;
        int count = 0;
        int ncols = colNames.length;

        while ((tuple = cube.next()) != null)
        {
            for (int i = 0; i < ncols; i++)
            {

                // print.f(String.format("Row %d, column %d. Expected=%s. Found=%s",
                // count,
                // i,
                // java.util.Arrays.toString(expected[count]),
                // tuple));

                Assert.assertEquals(tuple.get(i),
                                    expected[count][i],
                                    String.format("Row %d, column %d. Expected=%s. Found=%s",
                                                  count,
                                                  i,
                                                  java.util.Arrays.toString(expected[count]),
                                                  tuple));
            }
            count++;
        }

        Assert.assertEquals(count, expected.length);
    }

    @Override
    public BlockProperties getProperties()
    {
        BlockProperties props = new BlockProperties(null, schema, (BlockProperties) null);
        props.setBlockId(blockId);
        return props;
    }

}
