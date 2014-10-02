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

package com.linkedin.cubert.io.avro;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.CommonContext;

/**
 * Writes a block in AvroKey format (the tuples are stored as keys, and the values are
 * null).
 * 
 * @author Maneesh Varshney
 * 
 */
public class AvroBlockWriter implements BlockWriter
{
    // private final AvroKey<Record> avroKey = new AvroKey<Record>();
    // private Record record;
    // private int numColumns;
    // private BlockSchema outputSchema;
    // private Schema avroSchema;

    @Override
    public void configure(JsonNode json) throws IOException
    {
        // final BlockSchema schema = new BlockSchema(json.get("schema"));
        // outputSchema = schema;
        // numColumns = schema.getNumColumns();
        //
        // String relationName = JsonUtils.getText(json, "name");
        //
        // if (json.has("avroSchema"))
        // avroSchema =
        // new org.apache.avro.Schema.Parser().parse(JsonUtils.getText(json,
        // "avroSchema"));
        // else
        // avroSchema = AvroUtils.convertFromBlockSchema(relationName, schema);
        //
        // record = createRecord(avroSchema);
    }

    @Override
    public void write(Block block, CommonContext context) throws IOException,
            InterruptedException
    {
        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            context.write(null, tuple);
        }
    }

    // public Record createRecord(Schema avroSchema)
    // {
    // final Record record = new Record(avroSchema);
    // final List<Schema.Field> fields = record.getSchema().getFields();
    // for (int i = 0; i < fields.size(); ++i)
    // {
    // final Schema subSchema = fields.get(i).schema();
    // if (subSchema.getType() == Schema.Type.RECORD)
    // {
    // record.put(i, createRecord(subSchema));
    // }
    // else if (subSchema.getType() == Schema.Type.ARRAY)
    // {
    // /* Initialize array with 0 size */
    // record.put(i, new GenericData.Array(0, subSchema));
    // }
    // }
    // return record;
    // }

    // public void write2(Block block, CommonContext context) throws IOException,
    // InterruptedException
    // {
    // int[] outputFieldIndex = new int[numColumns];
    //
    // for (int i = 0; i < outputFieldIndex.length; i++)
    // {
    //
    // outputFieldIndex[i] =
    // block.getProperties().getSchema().getIndex(outputSchema.getName(i));
    // }
    //
    // Tuple tuple;
    // while ((tuple = block.next()) != null)
    // {
    // /* Reusing the same object */
    // for (int i = 0; i < numColumns; i++)
    // {
    // if (outputFieldIndex[i] >= tuple.size())
    // System.out.println("Malformed tuple " + tuple.toString());
    // final Object field = tuple.get(outputFieldIndex[i]);
    // final Schema fieldSchema = avroSchema.getFields().get(i).schema();
    // writeField(record, i, field, fieldSchema);
    // }
    // avroKey.datum(record);
    // context.write(avroKey, NullWritable.get());
    // }
    // }
    //
    // public void writeField(final Record record,
    // final int index,
    // Object value,
    // Schema schema) throws ExecException
    // {
    // /* Storing a tuple as a record object */
    // if (value instanceof Tuple)
    // {
    // writeRecordObject((Record) record.get(index), (Tuple) value, schema);
    // }
    // else if (value instanceof DataBag)
    // {
    // writeArrayObject((GenericData.Array) record.get(index),
    // (DataBag) value,
    // schema);
    // }
    // else
    // {
    // record.put(index, value);
    // }
    // }
    //
    // private void writeArrayObject(final GenericData.Array array,
    // final DataBag bag,
    // Schema schema) throws ExecException
    // {
    // array.clear();
    // if (schema.getType() != Schema.Type.ARRAY)
    // {
    // throw new RuntimeException("Unexpected Schema TYPE: " + schema.getType());
    // }
    // final Schema subSchema = schema.getElementType();
    // for (Object value : bag)
    // {
    // if (!(value instanceof Tuple))
    // {
    // throw new RuntimeException("Assumption Violated: DataBag contains items of type"
    // + value.getClass());
    // }
    //
    // final Tuple tuple = (Tuple) value;
    // if (subSchema.getType() != Schema.Type.RECORD)
    // {
    // throw new
    // RuntimeException("Assumption Violated: Couldn't find wrapper RECORD object "
    // + subSchema.getType());
    // }
    //
    // final Record record = createRecord(subSchema);
    // writeRecordObject(record, tuple, subSchema);
    // array.add(record);
    // }
    // }
    //
    // private void writeRecordObject(final Record record, final Tuple tuple, Schema
    // schema) throws ExecException
    // {
    // if (schema.getType() != Schema.Type.RECORD)
    // {
    // throw new RuntimeException("Unexpected Schema TYPE:" + schema.getType());
    // }
    // final List<Schema.Field> fields = record.getSchema().getFields();
    // for (int i = 0; i < fields.size(); ++i)
    // {
    // writeField(record, i, tuple.get(i), schema.getFields().get(i).schema());
    // }
    // }
}
