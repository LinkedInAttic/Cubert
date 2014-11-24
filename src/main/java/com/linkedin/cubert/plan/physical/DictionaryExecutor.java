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

import java.io.IOException;

/**
 * A special JobExecutor that generates and refreshes dictionary for a data set.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DictionaryExecutor extends JobExecutor
{

    public DictionaryExecutor(String json, boolean profileMode) throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        super(json, profileMode);
        // TODO Auto-generated constructor stub
    }
    // private Path outputDir;
    //
    // public DictionaryExecutor(String json, boolean profileMode) throws IOException,
    // ClassNotFoundException,
    // InstantiationException,
    // IllegalAccessException
    // {
    // super(json, profileMode);
    // }
    //
    // public boolean run(boolean verbose) throws IOException,
    // InterruptedException,
    // ClassNotFoundException
    // {
    // boolean status = job.waitForCompletion(verbose);
    //
    // if (status)
    // {
    // GenerateDictionary.mergeDictionaries(conf, outputDir);
    // }
    //
    // // fs.delete(new Path(outputDir, "tmp"), true);
    // return status;
    // }
    //
    // @Override
    // protected void configureJob() throws IOException,
    // ClassNotFoundException
    // {
    // setJobName();
    // setLibjars();
    // setHadoopConf();
    //
    // for (JsonNode map : root.path("map"))
    // {
    // setInput(((ObjectNode) map).get("input"));
    // }
    // conf.set(CubertStrings.JSON_MAP_OPERATOR_LIST, root.get("map").toString());
    //
    // setOutput();
    // conf.set(CubertStrings.JSON_OUTPUT, root.get("output").toString());
    //
    // // Cache the current dictionary, if available
    // Path currentDict = new Path(outputDir, "dictionary");
    // System.out.println("---------- " + currentDict.toString());
    // if (fs.exists(currentDict))
    // {
    // try
    // {
    // DistributedCache.addCacheFile(new URI(currentDict.toString()), conf);
    // conf.set(CubertStrings.DICTIONARY_RELATION, currentDict.toString());
    // }
    // catch (URISyntaxException e)
    // {
    // throw new RuntimeException(e);
    // }
    // }
    //
    // job.setMapperClass(GenerateDictionary.CreateDictionaryMapper.class);
    // job.setReducerClass(GenerateDictionary.CreateDictionaryReducer.class);
    //
    // job.setMapOutputKeyClass(Text.class);
    // job.setMapOutputValueClass(Text.class);
    // }
    //
    // @Override
    // protected void setOutput() throws JsonGenerationException,
    // JsonMappingException,
    // IOException
    // {
    // JsonNode output = get(root, "output");
    //
    // // set the output path
    // outputDir = new Path(getText(output, "path"));
    // Path outputPath = new Path(outputDir, "tmp");
    //
    // fs.delete(outputPath, true);
    // FileOutputFormat.setOutputPath(job, outputPath);
    //
    // // set the column type
    // List<ColumnType> columnTypes = new ArrayList<ColumnType>();
    //
    // for (JsonNode column : asArray(output, "columns"))
    // {
    // ColumnType type = new ColumnType();
    // type.setName(column.getTextValue());
    // type.setType("int");
    // columnTypes.add(type);
    // }
    //
    // // set avro job properties
    // AvroJob.setOutputKeySchema(job, GenerateDictionary.getSchema());
    // AvroJob.setOutputValueSchema(job, Schema.create(Type.NULL));
    // job.setOutputFormatClass(AvroKeyOutputFormat.class);
    // }

}
