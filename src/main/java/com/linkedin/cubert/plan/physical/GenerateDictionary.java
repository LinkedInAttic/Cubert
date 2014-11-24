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

package com.linkedin.cubert.plan.physical;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.cubert.utils.CodeDictionary;

/**
 * Executes a Map-Reduce job to generate and refresh dictionaries for the various columns
 * in a relation.
 * 
 * Dictionaries will be created for columns that have {@code isDictionaryEncoded} set to
 * {@code true}.
 * 
 * @author Maneesh Varshney
 * 
 */
public class GenerateDictionary
{
    // public static class CreateDictionaryMapper<K, V> extends Mapper<K, V, Text, Text>
    // {
    // private String[] columnNames;
    // private boolean[] isDictionaryField;
    // private List<Set<String>> emittedKeys = new ArrayList<Set<String>>();
    //
    // private TupleCreator tupleCreator;
    // private final Text reuseKey = new Text();
    // private final Text reuseValue = new Text();
    //
    // private String replaceNull = "";
    // private String defaultValue = null;
    // private boolean defaultEmmited = false;
    //
    // @Override
    // protected void setup(Context context) throws IOException,
    // InterruptedException
    // {
    // Configuration conf = context.getConfiguration();
    //
    // FileCache.initialize(conf);
    // // FileCache cache = FileCache.get();
    //
    // ObjectMapper mapper = new ObjectMapper();
    // ArrayNode mapCommands =
    // mapper.readValue(conf.get(CubertStrings.JSON_MAP_OPERATOR_LIST),
    // ArrayNode.class);
    // JsonNode inputJson = mapCommands.get(0).get("input");
    // JsonNode outputJson =
    // mapper.readValue(conf.get(CubertStrings.JSON_OUTPUT), JsonNode.class);
    //
    // if (inputJson.has("replaceNull"))
    // {
    // replaceNull = JsonUtils.getText(inputJson, "replaceNull");
    // }
    //
    // if (inputJson.has("defaultValue"))
    // {
    // defaultValue = JsonUtils.getText(inputJson, "defaultValue");
    // }
    //
    // tupleCreator = new AvroTupleCreator();
    // tupleCreator.setup(inputJson);
    //
    // // Load the previous dictionary, if available
    // Map<String, CodeDictionary> dictionaries = null;
    //
    // String previousDict = conf.get(CubertStrings.DICTIONARY_RELATION);
    // System.out.println(">>> previous dict " + previousDict);
    // if (previousDict != null)
    // {
    // // String filename = cache.getCachedFile(previousDict);
    // // dictionaries = loadDictionary(filename, false, null);
    // }
    //
    // String outputColumns = JsonUtils.getText(outputJson, "columns");
    // BlockSchema inputSchema = new BlockSchema(inputJson.get("schema"));
    // BlockSchema outputSchema = new BlockSchema(outputColumns);
    // Set<String> dictionaryColumnSet = outputSchema.asMap().keySet();
    //
    // int numInputColumns = inputSchema.getNumColumns();
    // columnNames = inputSchema.getColumnNames();
    //
    // isDictionaryField = new boolean[numInputColumns];
    // for (int i = 0; i < numInputColumns; i++)
    // {
    // String colName = inputSchema.getName(i);
    // isDictionaryField[i] = dictionaryColumnSet.contains(colName);
    //
    // if (isDictionaryField[i])
    // {
    // Set<String> emitted = new HashSet<String>();
    // emittedKeys.add(emitted);
    //
    // if (dictionaries != null && dictionaries.containsKey(colName))
    // {
    // // Add the existing keys to emittedKeys, so that we don't emit
    // // them again
    // CodeDictionary dict = dictionaries.get(colName);
    // emittedKeys.get(i).addAll(dict.keySet());
    // }
    // }
    // else
    // {
    // emittedKeys.add(null);
    // }
    // }
    // }
    //
    // @Override
    // protected void map(K key, V value, Context context) throws IOException,
    // InterruptedException
    // {
    // map(tupleCreator.create(key, value), context);
    // }
    //
    // void map(Tuple tuple, Context context) throws IOException,
    // InterruptedException
    // {
    // for (int i = 0; i < isDictionaryField.length; i++)
    // {
    //
    // if (!isDictionaryField[i])
    // continue;
    //
    // Object val = tuple.get(i);
    // String colValue;
    //
    // if (val == null)
    // colValue = replaceNull;
    // else
    // colValue = val.toString();
    //
    // if (emittedKeys.get(i).contains(colValue))
    // {
    // continue;
    // }
    //
    // String colName = columnNames[i];
    // emitKeyValue(colName, colValue, context, i);
    //
    // if (defaultValue != null && !defaultEmmited)
    // emitKeyValue(colName, defaultValue, context, i);
    // }
    // defaultEmmited = true;
    // }
    //
    // void emitKeyValue(String colName,
    // String colValue,
    // Context context,
    // int columnIndex) throws IOException,
    // InterruptedException
    // {
    // reuseKey.set(colName);
    // reuseValue.set(colValue);
    // context.write(reuseKey, reuseValue);
    // emittedKeys.get(columnIndex).add(colValue);
    // }
    // }

    // public static class CreateDictionaryReducer extends
    // Reducer<Text, Text, AvroKey<Record>, NullWritable>
    // {
    // Record record;
    //
    // @Override
    // protected void setup(Context context) throws IOException,
    // InterruptedException
    // {
    // Configuration conf = context.getConfiguration();
    // Schema keySchema = AvroJob.getOutputKeySchema(conf);
    //
    // record = new Record(keySchema);
    // }
    //
    // @Override
    // protected void reduce(Text key, Iterable<Text> values, Context context) throws
    // IOException,
    // InterruptedException
    // {
    // record.put("colname", key.toString());
    // record.put("code", -1);
    //
    // for (Text value : values)
    // {
    // record.put("colvalue", value.toString());
    // context.write(new AvroKey<Record>(record), NullWritable.get());
    // }
    // }
    //
    // }

    public static Map<String, CodeDictionary> loadDictionary(String path,
                                                             boolean isHDFS,
                                                             Configuration conf) throws IOException
    {
        Map<String, CodeDictionary> dictionaries = new HashMap<String, CodeDictionary>();
        Schema schema = getSchema();

        DatumReader<GenericRecord> datumReader =
                new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader;

        if (isHDFS)
        {
            dataFileReader =
                    new DataFileReader<GenericRecord>(new FsInput(new Path(path), conf),
                                                      datumReader);
        }
        else
        {
            dataFileReader =
                    new DataFileReader<GenericRecord>(new File(path), datumReader);
        }
        GenericRecord record = null;
        while (dataFileReader.hasNext())
        {
            record = dataFileReader.next();
            String colName = record.get("colname").toString();
            String colValue = record.get("colvalue").toString();
            int code = (Integer) record.get("code");

            CodeDictionary dict = dictionaries.get(colName);
            if (dict == null)
            {
                dict = new CodeDictionary();
                dictionaries.put(colName, dict);
            }

            dict.addKeyCode(colValue, code);
        }

        dataFileReader.close();

        return dictionaries;
    }

    public static void mergeDictionaries(Configuration conf, Path dir) throws IOException
    {
        Map<String, CodeDictionary> dictionaries = new HashMap<String, CodeDictionary>();
        FileSystem fs = FileSystem.get(conf);

        Path currentDictPath = new Path(dir, "dictionary");
        Schema schema = getSchema();

        // Read the existing dictionaries
        if (fs.exists(currentDictPath))
        {
            dictionaries.putAll(loadDictionary(currentDictPath.toString(), true, conf));

            // move the current dictionary to new file
            Path oldPath = new Path(dir, "_dictionary.old");
            fs.delete(oldPath, true);
            fs.rename(currentDictPath, oldPath);
        }

        // Read the new entries
        Path globPath = new Path(dir, "tmp/part-*");
        FileStatus[] allStatus = fs.globStatus(globPath);
        for (FileStatus status : allStatus)
        {
            DatumReader<GenericRecord> datumReader =
                    new GenericDatumReader<GenericRecord>(schema);
            DataFileReader<GenericRecord> dataFileReader =
                    new DataFileReader<GenericRecord>(new FsInput(status.getPath(), conf),
                                                      datumReader);
            GenericRecord record = null;
            while (dataFileReader.hasNext())
            {
                record = dataFileReader.next();
                String colName = record.get("colname").toString();
                String colValue = record.get("colvalue").toString();

                CodeDictionary dict = dictionaries.get(colName);
                if (dict == null)
                {
                    dict = new CodeDictionary();
                    dictionaries.put(colName, dict);
                }

                dict.addKey(colValue);
            }
        }

        // Write the dictionaries back
        DatumWriter<GenericRecord> datumWriter =
                new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<GenericRecord>(datumWriter);
        FSDataOutputStream out = fs.create(currentDictPath);

        dataFileWriter.create(schema, out);
        Record record = new Record(schema);

        for (Map.Entry<String, CodeDictionary> entry : dictionaries.entrySet())
        {
            String colName = entry.getKey();
            CodeDictionary dict = entry.getValue();

            for (String colValue : dict.keySet())
            {
                int code = dict.getCodeForKey(colValue);
                record.put("colname", colName);
                record.put("colvalue", colValue);
                record.put("code", code);

                dataFileWriter.append(record);
            }
        }
        dataFileWriter.close();

    }

    public static Schema getSchema()
    {
        Field[] fields =
                {
                        new Schema.Field("colname",
                                         Schema.create(Type.STRING),
                                         null,
                                         null),
                        new Schema.Field("colvalue",
                                         Schema.create(Type.STRING),
                                         null,
                                         null),
                        new Schema.Field("code", Schema.create(Type.INT), null, null) };

        Schema schema = Schema.createRecord("dictionary", null, null, false);
        schema.setFields(Arrays.asList(fields));

        return schema;
    }
}
