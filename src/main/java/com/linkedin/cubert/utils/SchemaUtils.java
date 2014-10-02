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

package com.linkedin.cubert.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;

public class SchemaUtils
{
    public static ColumnType coltypeFromFieldSchema(String colName, FieldSchema colSchema)
    {
        ColumnType t = new ColumnType();
        t.setName(colName);
        t.setType(convertoRCFTypeName(DataType.findTypeName(colSchema.type)));
        if (colSchema.schema != null)
        {
            try
            {
                t.setColumnSchema(convertToBlockSchema(colSchema.schema));
            }
            catch (FrontendException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return t;
    }

    public static BlockSchema fieldSchemaToBlockSchema(FieldSchema fschema)
    {
        ColumnType[] ctypes = new ColumnType[1];
        ColumnType ct = coltypeFromFieldSchema(fschema.alias, fschema);
        ctypes[0] = ct;
        return new BlockSchema(ctypes);
    }

    public static BlockSchema convertToBlockSchema(Schema schema) throws FrontendException
    {
        ColumnType[] ctypes = new ColumnType[schema.size()];
        for (int i = 0; i < ctypes.length; i++)
        {
            ColumnType ct = new ColumnType();
            FieldSchema fs = schema.getField(i);

            ct.setName(fs.alias);
            ct.setType(convertoRCFTypeName(DataType.findTypeName(fs.type)));
            if (fs.schema != null)
            {
                ct.setColumnSchema(convertToBlockSchema(fs.schema));
            }

            ctypes[i] = ct;
        }

        return new BlockSchema(ctypes);
    }

    public static Schema convertFromBlockSchema(BlockSchema blockSchema) throws FrontendException
    {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        for (int i = 0; i < blockSchema.getNumColumns(); i++)
        {
            ColumnType ctype = blockSchema.getColumnType(i);
            byte pigtype = convertToPigType(ctype.getType().toString());
            if (ctype.getColumnSchema() != null)
            {
                Schema nestedSchema = convertFromBlockSchema(ctype.getColumnSchema());
                fieldSchemas.add(new FieldSchema(ctype.getName(), nestedSchema, pigtype));
            }
            else
                fieldSchemas.add(new FieldSchema(ctype.getName(), pigtype));
        }
        return new Schema(fieldSchemas);
    }

    public static byte convertToPigType(String rcfTypeName)
    {

        String pigTypeString;
        if (rcfTypeName.equals("STRING"))
            pigTypeString = ("CHARARRAY");
        else if (rcfTypeName.equals("INT"))
            pigTypeString = "INTEGER";
        else if (rcfTypeName.equals("RECORD"))
            pigTypeString = "TUPLE";
        else if (rcfTypeName.equals("ARRAY"))
            pigTypeString = "BAG";
        else if (rcfTypeName.equals("BYTES"))
            pigTypeString = "BYTEARRAY";
        else
            pigTypeString = (rcfTypeName);

        return DataType.genNameToTypeMap().get(pigTypeString);

    }

    public static String convertoRCFTypeName(String pigTypeName)
    {
        String rcfTypeName = pigTypeName;

        if (pigTypeName.equals("chararray"))
            rcfTypeName = "STRING";
        else if (pigTypeName.equals("bytearray"))
            rcfTypeName = "BYTES";

        return rcfTypeName;

    }

}
