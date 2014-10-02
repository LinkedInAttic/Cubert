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

package com.linkedin.cubert.io.text;

import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.block.TupleCreator;

/**
 * 
 * Creates tuples from text input. The separator is read from the json.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class TextTupleCreator implements TupleCreator
{
    private DataType[] typeArray;
    private BlockSchema schema;
    Tuple tuple;
    private String separator =
            new String(new byte[] { PigTextOutputFormatWrapper.defaultDelimiter });

    @Override
    public void setup(JsonNode json) throws IOException
    {
        if (json.has("params"))
        {
            JsonNode params = json.get("params");
            if (params.has("separator"))
            {
                String str = params.get("separator").getTextValue();
                str = StringEscapeUtils.unescapeJava(str);
                byte[] bytes = str.getBytes("UTF-8");
                separator = new String(bytes);
            }
        }

        schema = new BlockSchema(json.get("schema"));
        typeArray = new DataType[schema.getNumColumns()];
        for (int i = 0; i < schema.getNumColumns(); i++)
            typeArray[i] = schema.getType(i);

        tuple = TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }

    @Override
    public Tuple create(Object key, Object value) throws ExecException
    {
        Text t = (Text) value;
        String[] fields = t.toString().split(separator);

        for (int i = 0; i < fields.length; i++)
        {
            Object obj = null;

            if (fields[i] != null && fields[i].length() != 0)
                switch (typeArray[i])
                {
                case INT:
                    obj = new Integer(Integer.parseInt(fields[i]));
                    break;

                case LONG:
                    obj = new Long(Long.parseLong(fields[i]));
                    break;

                case STRING:
                    obj = fields[i];
                    break;

                case DOUBLE:
                    obj = Double.parseDouble(fields[i]);
                    break;

                case FLOAT:
                    obj = Float.parseFloat(fields[i]);
                    break;
                default:
                    break;
                }

            tuple.set(i, obj);
        }

        return tuple;
    }
}
