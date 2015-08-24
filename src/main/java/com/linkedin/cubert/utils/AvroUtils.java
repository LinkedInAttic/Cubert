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

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.pig.piggybank.storage.avro.AvroSchema2Pig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;


/**
 * Various utility methods related to Avro Schema.
 * 
 * @author Maneesh Varshney
 * 
 */
public class AvroUtils
{
    private static int arrayElemInSchemaCounter = 0;
    private static final boolean PadDefaultNullsToSchema = true;

    /**
     * Extracts the schema of an Avro file.
     * 
     * @param conf
     * @param path
     * @return
     * @throws IOException
     */
    public static Schema getSchema(Configuration conf, Path path) throws IOException
    {
        FileSystem fs = path.getFileSystem(conf);

        Path anAvroFile = FileSystemUtils.getFirstMatch(fs, path, "*.avro", true);

        if (anAvroFile == null)
            throw new IOException("there are no files in " + path.toString());

        System.out.println("Obtaining schema of avro file " + anAvroFile.toString());

        return getSchema(new FsInput(anAvroFile, conf));
    }

    public static Schema getSchema(SeekableInput input) throws IOException
    {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<GenericRecord>(input, datumReader);
        Schema schema = dataFileReader.getSchema();

        if (PadDefaultNullsToSchema)
        {
            // a list of "cloned" fields, with optional default value set to null
            ArrayList<Field> paddedFields = new ArrayList<Field>();

            for (Field field: schema.getFields())
            {
                // should this field be padded?
                boolean needsNullPadding = (field.schema() != null) // the field has nested schema
                    && (field.schema().getType().equals(Type.UNION)) // the nested schema is UNION
                    && (field.schema().getTypes().get(0).getType().equals(Type.NULL)); // the first element of union is NULL type

                JsonNode defValue = needsNullPadding ? NullNode.getInstance() : field.defaultValue();

                Field f = new Field(field.name(), field.schema(), field.doc(), defValue);
                paddedFields.add(f);
            }

            schema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
            schema.setFields(paddedFields);
        }

        return schema;
    }

    /**
     * Converts a ColumnType array to Avro's Schema.
     * 
     * @param recordName
     * @param schema
     * @return
     */
    public static Schema convertFromBlockSchema(String recordName, BlockSchema schema)
    {
        arrayElemInSchemaCounter = 0;
        return convertFromBlockSchema(recordName, Type.RECORD, schema, true);
    }

    private static Field[] createFields(BlockSchema schema){
      Field[] fields = new Field[schema.getNumColumns()];
      for (int idx = 0; idx < fields.length; idx++)
      {
        final ColumnType col = schema.getColumnType(idx);
        final DataType colType = col.getType();
        final Type subType = convertToAvroType(colType);
        
        final Schema colSchema;
        if (col.getColumnSchema() != null ||
            subType == Type.ARRAY || subType == Type.MAP)
        {
          colSchema =
            convertFromBlockSchema(col.getName(),
                                   subType,
                                   col.getColumnSchema(), false);
          
        }
        else
        {
          List<Schema> unionSchema = new ArrayList<Schema>();
          unionSchema.add(Schema.create(Type.NULL));
          unionSchema.add(Schema.create(subType));
          
          colSchema = Schema.createUnion(unionSchema);
        }
        fields[idx] = new Field(col.getName(), colSchema, null, null);
      }
      return fields;
    }

    private static Schema convertFromBlockSchema(final String name,
                                                 final Type type,
                                                 final BlockSchema schema,
                                                 boolean toplevel)
    {

        Schema avroSchema;
        switch (type)
        {
        case RECORD:
            Field[] fields = createFields(schema);
            avroSchema = Schema.createRecord(name, null, null, false);
            avroSchema.setFields(Arrays.asList(fields));
            if (toplevel)
              break;
            List<Schema> unionSchema = new ArrayList<Schema>();
            unionSchema.add(Schema.create(Type.NULL));
            unionSchema.add(avroSchema);
            avroSchema = Schema.createUnion(unionSchema);
            break;
        case ARRAY:
        {
            if (schema.getNumColumns() != 1)
            {
                throw new RuntimeException("Type ARRAY must have a single element in the subschema");
            }

            ColumnType elemColType = schema.getColumnType(0);
            Schema elemType;
            if (elemColType.getColumnSchema() == null)
            {
                elemType = Schema.create(convertToAvroType(elemColType.getType()));
            }
            else
            {
                elemType =
                        convertFromBlockSchema(elemColType.getName() + (arrayElemInSchemaCounter++),
                                               convertToAvroType(elemColType.getType()),
                                               elemColType.getColumnSchema(), false);
            }

            avroSchema = Schema.createArray(elemType);

            unionSchema = new ArrayList<Schema>();
            unionSchema.add(Schema.create(Type.NULL));
            unionSchema.add(avroSchema);
            avroSchema = Schema.createUnion(unionSchema);

            break;
        }
        case MAP:
        {
            ColumnType valueColType = schema.getColumnType(0);
            Schema valueType;
            if (valueColType.getColumnSchema() == null)
            {
                valueType = Schema.create(convertToAvroType(valueColType.getType()));
            }
            else
            {
                valueType =
                        convertFromBlockSchema(valueColType.getName(),
                                               convertToAvroType(valueColType.getType()),
                                               valueColType.getColumnSchema(), false);
            }
            avroSchema = Schema.createMap(valueType);

            unionSchema = new ArrayList<Schema>();
            unionSchema.add(Schema.create(Type.NULL));
            unionSchema.add(avroSchema);
            avroSchema = Schema.createUnion(unionSchema);

            break;
        }
        default:
            throw new IllegalArgumentException("Unsupported composite Type: " + type);
        }
        return avroSchema;
    }

    private static Type convertToAvroType(DataType colType)
    {
        final Type subType;
        if (colType == DataType.TUPLE)
        {
            /* Pig converts RECORD to TUPLE. Converting it back. */
            subType = Type.RECORD;
        }
        else if (colType == DataType.BAG)
        {
            subType = Type.ARRAY;
        }
        else if (colType == DataType.MAP)
        {
          subType = Type.MAP;
        }
        else
        {
            subType = Type.valueOf(colType.toString().toUpperCase());
        }
        return subType;
    }

    // Convert to Pig Schema use the utility functions in Pig first, then convert to Block
    // Schema.
    // Thus only the Pig <-> Cubert schema conversion path is required to be maintained in
    // the code.
    public static BlockSchema convertToBlockSchema(Schema avroSchema)
    {
        try
        {
            org.apache.pig.ResourceSchema pigResourceSchema =
                    AvroSchema2Pig.convert(avroSchema);
            org.apache.pig.impl.logicalLayer.schema.Schema pigSchema =
                    org.apache.pig.impl.logicalLayer.schema.Schema.getPigSchema(pigResourceSchema);
            return SchemaUtils.convertToBlockSchema(pigSchema);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void createFileIfNotExists(BlockSchema fileSchema, String path) throws IOException
    {
        Configuration conf = new JobConf();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(path)))
            return;

        Schema avroSchema = convertFromBlockSchema("CUBERT_MV_RECORD", fileSchema);
        System.out.println("Creating avro file with schema = " + avroSchema);
        GenericDatumWriter<GenericRecord> datumWriter =
                new GenericDatumWriter<GenericRecord>(avroSchema);
        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(datumWriter);

        FSDataOutputStream fout =
                FileSystem.create(fs,
                                  new Path(path),
                                  new FsPermission(FsAction.ALL,
                                                   FsAction.READ_EXECUTE,
                                                   FsAction.READ_EXECUTE));
        writer.create(avroSchema, fout);
        writer.flush();
        writer.close();

    }

    public static void main(String[] args) throws IOException
    {
        JobConf conf = new JobConf();

        System.out.println(AvroUtils.getSchema(conf, new Path(args[0])).toString(true));
    }
}
