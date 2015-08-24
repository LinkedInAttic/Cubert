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

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.io.*;
import com.linkedin.cubert.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Parses and executes the physical plan of a single Map-Reduce job.
 *
 * @author Maneesh Varshney
 */
public class JobExecutor
{
    protected final JsonNode root;
    protected static final ArrayNode singletonArray =
            new ObjectMapper().createArrayNode();
    protected final Job job;
    protected final Configuration conf;
    protected final ConfigurationDiff confDiff;
    protected final FileSystem fs;
    private final ObjectMapper mapper;
    private final Path tmpDir;

    // A map of folder name to file name prefix
    private final Map<String, List<String>> teeFiles =
            new HashMap<String, List<String>>();
    private int teeFilePrefixCounter = 0;

    private boolean profileMode;

    public JobExecutor(String json, boolean profileMode) throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        this.job = new Job();
        this.conf = job.getConfiguration();
        this.confDiff = new ConfigurationDiff(conf);
        this.fs = FileSystem.get(conf);
        this.profileMode = profileMode;

        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);

        job.setJarByClass(JobExecutor.class);
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null)
        {

            conf.set("mapreduce.job.credentials.binary",
                     System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        mapper = new ObjectMapper();
        this.root = mapper.readValue(json, JsonNode.class);

        if (root.has("tmpDir"))
        {
            tmpDir = new Path(getText(root, "tmpDir"));
        }
        else
        {
            tmpDir =
                    new Path(fs.getHomeDirectory(), "tmp/" + UUID.randomUUID().toString());
        }

        try
        {
            configureJob();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }

    }

    public void printCubertConfProperties()
    {
        Iterator<Entry<String, String>> it = conf.iterator();
        while (it.hasNext())
        {
            Entry<String, String> entry = it.next();
            if (entry.getKey().startsWith("cubert"))
            {
                print.f("%s => %s", entry.getKey(), entry.getValue());
            }
        }
    }

    public boolean run(boolean verbose) throws IOException,
            InterruptedException,
            ClassNotFoundException
    {
        // Logger.getLogger("org.apache.hadoop.mapred.MapTask").setLevel(Level.WARN);
        // Logger.getLogger("org.apache.hadoop.mapred.Task").setLevel(Level.WARN);
        // Logger.getLogger("org.apache.hadoop.mapred.Merger").setLevel(Level.WARN);
        // Logger.getLogger("org.apache.hadoop.mapred.LocalJobRunner").setLevel(Level.WARN);
        // Logger.getLogger("org.apache.hadoop.filecache.TrackerDistributedCacheManager")
        // .setLevel(Level.WARN);

        boolean retval = false;
        try
        {
            retval = job.waitForCompletion(verbose);
        }
        finally
        {
            fs.delete(tmpDir, true);
        }

        if (!retval)
        {
            throw new InterruptedException("Job " + getText(root, "name") + " failed!");
        }

        moveTeeFiles();
        postJobHooks();
        doCompletionTasks();

        return retval;
    }

    protected void configureJob() throws IOException,
            ClassNotFoundException,
            URISyntaxException,
            InstantiationException,
            IllegalAccessException
    {
        setJobName();
        setLibjars();
        setHadoopConf();
        setPerfProfile();
        serializeExecutionConfig();
        cacheFiles();
        cacheIndex();
        prepareTeePaths();
        preJobHooks();

        int numReducers = root.get("reducers").getIntValue();
        job.setNumReduceTasks(numReducers);

        boolean foundPaths = false;
        for (JsonNode map : root.path("map"))
        {
            foundPaths |= setInput(((ObjectNode) map).get("input"));
        }
        if (!foundPaths)
          throw new IOException("Cannot find any input paths for job");

        setOutput();
        conf.set(CubertStrings.JSON_OUTPUT, root.get("output").toString());
        if (root.has("metadata"))
            conf.set(CubertStrings.JSON_METADATA, root.get("metadata").toString());

        conf.set(CubertStrings.JSON_MAP_OPERATOR_LIST, root.get("map").toString());
        job.setMapperClass(CubertMapper.class);

        if (hasReducePhase())
        {
            setShuffle();
            conf.set(CubertStrings.JSON_SHUFFLE, root.get("shuffle").toString());

            conf.set(CubertStrings.JSON_REDUCE_OPERATOR_LIST, root.get("reduce")
                                                                  .toString());
            job.setReducerClass(CubertReducer.class);

        }

        if (conf.get("mapreduce.map.output.compress") == null)
            conf.set("mapreduce.map.output.compress", "true");

        if (conf.get("mapreduce.output.fileoutputformat.compress") == null)
            conf.set("mapreduce.output.fileoutputformat.compress", "true");
    }

    public boolean hasReducePhase()
    {
        return root.has("shuffle") && !root.get("shuffle").isNull();
    }

    private void serializeExecutionConfig() throws IOException
    {
        ExecutionConfig.writeConf(getConf());
    }

    private void preJobHooks()
    {
        ArrayNode preHooks = (ArrayNode) root.get("preJobHooks");
        if (preHooks != null)
            processJobCommands(preHooks);

    }

    private void postJobHooks()
    {
        ArrayNode postHooks = (ArrayNode) root.get("postJobHooks");
        if (postHooks != null)
            processJobCommands(postHooks);

    }

    private void doCompletionTasks() throws IOException
    {
        if (root.has("onCompletion") && !root.get("onCompletion").isNull())
            CompletionTasks.doCompletionTasks(root.get("onCompletion"));
    }

    private void processJobCommands(ArrayNode commands)
    {
        for (int i = 0; i < commands.size(); i++)
        {
            execJobCommand(commands.get(i));
        }

    }

    private void execJobCommand(JsonNode jsonNode)
    {
     // TODO Auto-generated method stub
      String[] commandSplits = jsonNode.getTextValue().split("\\s+");
      String command = commandSplits[0];

      try {
        if (command.equalsIgnoreCase("METAFILE")){
          String[] metadataArgs = Arrays.copyOfRange(commandSplits, 1, commandSplits.length);
          CubertMD.execCommand(metadataArgs);
        }
        else if (command.equalsIgnoreCase("HDFS")){
          execHdfsCommand(commandSplits[1], Arrays.copyOfRange(commandSplits, 2, commandSplits.length));
        }

      }
      catch (IOException e){
        throw new RuntimeException("Job command failed due to " + e.toString());
      }

    }

    private void execHdfsCommand(String cmd, String[] args) throws IOException{
       FileSystem fs = FileSystem.get(conf);
       if (cmd.equalsIgnoreCase("RENAME"))
         fs.rename(new Path(args[0]), new Path(args[1]));
       else if (cmd.equalsIgnoreCase("DELETE"))
         fs.delete(new Path(args[0]));

    }



    protected void setJobName()
    {
        job.setJobName(getText(root, "name"));
    }

    protected void setLibjars() throws IOException
    {
        if (!root.has("libjars"))
            return;

        FileSystem localFs = FileSystem.getLocal(conf);

        HashSet<Path> jarsToCache = new HashSet<Path>();

        for (JsonNode node : asArray(root, "libjars"))
        {
            Path path = new Path(node.getTextValue());

            // Is path in local fs or HDFS
            boolean fileIsLocal = localFs.exists(path);
            FileSystem thisFs = fileIsLocal ? localFs : fs;

            // If path is a directory, glob all jar files.
            List<Path> sources = new LinkedList<Path>();
            if (thisFs.isDirectory(path))
            {
                Path dirPath = new Path(path.toString() + "/*.jar");
                FileStatus[] jars = thisFs.globStatus(dirPath);

                for (FileStatus jar : jars)
                {
                    Path filePath = jar.getPath();
                    sources.add(filePath);
                }
            }
            else
            {
                sources.add(path);
            }

            // For all source jars corresponding to this <code>path</code>
            // add to HDFS if path is local
            for (Path srcPath : sources)
            {
                if (fileIsLocal)
                {
                    Path dstPath = new Path(tmpDir, srcPath.getName());
                    fs.copyFromLocalFile(srcPath, dstPath);
                    srcPath = dstPath;
                }

                if (jarsToCache.contains(srcPath))
                {
                    throw new RuntimeException("Duplicate jar specified: '" + srcPath.getName() + "'");
                }
                jarsToCache.add(srcPath);
            }
        }

        // Add jars to distributed cache
        for (Path path : jarsToCache)
        {
            DistributedCache.addFileToClassPath(path, conf, fs);
        }
    }

    protected void setHadoopConf()
    {
        if (!root.has("hadoopConf"))
            return;

        JsonNode node = get(root, "hadoopConf");
        Iterator<String> it = node.getFieldNames();
        while (it.hasNext())
        {
            String name = it.next();
            String value = getText(node, name);

            conf.set(name, value);
        }
    }

    protected void setPerfProfile()
    {
        conf.set(CubertStrings.PROFILE_MODE, profileMode ? "true" : "false");
    }

    protected void cacheFiles() throws URISyntaxException,
            IOException
    {
        if (!root.has("cachedFiles") || root.get("cachedFiles").isNull()
                || root.get("cachedFiles").size() == 0)
            return;

        for (JsonNode cachedFile : root.path("cachedFiles"))
        {
            URI uri = new URI(cachedFile.getTextValue());
            print.f("CACHING file %s", uri);
            DistributedCache.addCacheFile(uri, conf);
        }
    }

    protected void cacheIndex() throws IOException,
            InstantiationException,
            IllegalAccessException,
            ClassNotFoundException,
            URISyntaxException
    {
        if (!root.has("cacheIndex"))
            return;

        HashMap<String, Path> cachedIndexFiles = new HashMap<String, Path>();


        for (JsonNode indexNode : root.path("cacheIndex"))
        {
            final String origPathName = getText(indexNode, "path");
            final String indexName = JsonUtils.getText(indexNode, "name");

            // Reuse index (to be put into distributed cache) if already created.
            Path indexPath = cachedIndexFiles.get(origPathName);

            if (indexPath == null)
            {
                // extract the index named by "index" from the location specified in "path";
                Index indexToCache = Index.extractFromRelation(conf, origPathName);


                indexPath = new Path(tmpDir, UUID.randomUUID().toString());
                SerializerUtils.serializeToFile(conf, indexPath, indexToCache);

                cachedIndexFiles.put(origPathName, indexPath);
            }

            DistributedCache.addCacheFile(new URI(indexPath.toString() + "#" + indexName), conf);

            conf.set(CubertStrings.JSON_CACHE_INDEX_PREFIX + indexName, indexPath.getName());

            print.f("Caching index at path [%s] as [%s]", origPathName, indexPath.toString());
        }

    }

    protected boolean setInput(JsonNode input) throws IOException,
            ClassNotFoundException
    {
        JsonNode params = input.get("params");
        if (params == null)
            params = mapper.createObjectNode();
        // RelationType type = RelationType.valueOf(getText(input, "type"));
        List<Path> paths = FileSystemUtils.getPaths(fs, input.get("path"), params);

        if (paths.isEmpty())
        {
          return false;
        }

        job.setInputFormatClass(CubertInputFormat.class);

        // storage specific configuration
        confDiff.startDiff();

        Storage storage = StorageFactory.get(getText(input, "type"));

        storage.prepareInput(job, conf, params, paths);

        if (params.has("combined") && Boolean.parseBoolean(getText(params, "combined")))
        {
            conf.setBoolean(CubertStrings.COMBINED_INPUT, true);

            long originalMaxCombinedSplitSize =
                    conf.getLong("mapreduce.input.fileinputformat.split.maxsize", -1);

            if (originalMaxCombinedSplitSize == -1)
            {
                throw new IllegalStateException("CONFIG ERROR: property mapreduce.input.fileinputformat.split.maxsize is not set when using combined input format");
            }
        }

        // add input paths to the job
        FileInputFormat.setInputPaths(job, paths.toArray(new Path[] {}));

        confDiff.endDiff();
        return true;
    }

    protected void setOutput() throws IOException
    {
        JsonNode output = get(root, "output");
        JsonNode params = output.get("params");
        if (params == null)
            params = mapper.createObjectNode();

        Path outputPath = new Path(getText(output, "path"));
        FileOutputFormat.setOutputPath(job, outputPath);

        if (params.has("overwrite") && Boolean.parseBoolean(getText(params, "overwrite")))
        {
            fs.delete(outputPath, true);
        }

        BlockSchema schema = new BlockSchema(output.get("schema"));

        Storage storage = StorageFactory.get(getText(output, "type"));
        storage.prepareOutput(job, conf, params, schema, outputPath);
    }

    protected void setShuffle()
    {
        JsonNode shuffle = get(root, "shuffle");

        setPartitioner(shuffle);

        Storage storage = StorageFactory.get(getText(shuffle, "type"));
        storage.prepareOutput(job, conf, null, null, null);

        if (shuffle.has("aggregates"))
        {
            job.setCombinerClass(CubertCombiner.class);
        }
    }

    private void setPartitioner(JsonNode shuffle)
    {
        Class<? extends Partitioner> partitionerClass = null;

        String mrPartitioner = getConf().get("mapreduce.partitioner.class");
        if (mrPartitioner != null) {
            try
            {
                partitionerClass = Class.forName(mrPartitioner).asSubclass(Partitioner.class);
            } catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }

        } else if (shuffle.has("partitionerClass"))
        {
            try
            {
                partitionerClass = Class.forName(getText(shuffle, "partitionerClass")).asSubclass(Partitioner.class);

                job.setPartitionerClass(partitionerClass);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            partitionerClass = CubertPartitioner.class;
        }

        print.f("Setting partitioner: " + partitionerClass.getName());
        job.setPartitionerClass(partitionerClass);
    }

    protected void setNumReducers(int numReducers)
    {
        job.setNumReduceTasks(numReducers);

    }

    protected void setCompression(Class<? extends CompressionCodec> codecClass)
    {
        if (codecClass != null)
        {
            conf.setBoolean("mapred.output.compress", true);
            conf.setClass("mapred.output.compression.codec",
                          codecClass,
                          CompressionCodec.class);
        }

    }

    private void prepareTeePaths()
    {
        for (JsonNode mapNode : root.path("map"))
        {
            prepareTeePaths(mapNode.get("operators"));
        }

        if (hasReducePhase())
        {
            prepareTeePaths(root.get("reduce"));
        }
    }

    private void prepareTeePaths(JsonNode operators)
    {
        for (JsonNode operatorNode : operators)
        {
            String name = operatorNode.get("operator").getTextValue();
            if (name.equals("TEE"))
            {
                String path = operatorNode.get("path").getTextValue();

                String teePrefix = String.format("tee-%04d", teeFilePrefixCounter++);
                ((ObjectNode) operatorNode).put("prefix", teePrefix);
                List<String> prefixList = teeFiles.get(path);
                if (prefixList == null)
                {
                    prefixList = new ArrayList<String>();
                    teeFiles.put(path, prefixList);
                }
                prefixList.add(teePrefix);
            }
        }
    }

    private void moveTeeFiles() throws IOException
    {
        if (teeFiles.size() == 0)
            return;

        Path outputDir = new Path(root.get("output").get("path").getTextValue());

        for (String dir : teeFiles.keySet())
        {
            // delete the old directory
            Path teeDir = new Path(dir);
            if (fs.exists(teeDir))
                fs.delete(teeDir, true);

            fs.mkdirs(teeDir);

            for (String prefix : teeFiles.get(dir))
            {
                Path globPath = new Path(outputDir, prefix + "*");
                FileStatus[] fileStatusList = fs.globStatus(globPath);
                for (FileStatus fileStatus : fileStatusList)
                {
                    fs.rename(fileStatus.getPath(), teeDir);
                }
            }

        }
    }

    protected Job getJob()
    {
        return job;
    }

    protected Configuration getConf()
    {
        return conf;
    }

    public static JsonNode get(JsonNode node, String property)
    {
        JsonNode val = node.get(property);
        if (val == null)
        {
            throw new IllegalArgumentException("Property " + property
                    + " is not defined in " + node);
        }
        return val;
    }

    public static String getText(JsonNode node, String property, String defaultValue)
    {
        if (!node.has(property))
            return defaultValue;
        return get(node, property).getTextValue();
    }

    public static String getText(JsonNode node, String property)
    {
        return get(node, property).getTextValue();
    }

    public static JsonNode asArray(JsonNode node, String property)
    {
        JsonNode n = node.get(property);
        if (n.isArray())
            return node.path(property);
        else
        {
            singletonArray.removeAll();
            singletonArray.add(n);
            return singletonArray;
        }
    }
}
