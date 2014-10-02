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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.io.ConfigurationDiff;
import com.linkedin.cubert.io.CubertInputFormat;
import com.linkedin.cubert.io.SerializerUtils;
import com.linkedin.cubert.io.Storage;
import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.utils.ExecutionConfig;
import com.linkedin.cubert.utils.FileSystemUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.print;

/**
 * Parses and executes the physical plan of a single Map-Reduce job.
 * 
 * @author Maneesh Varshney
 * 
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

        for (JsonNode map : root.path("map"))
        {
            setInput(((ObjectNode) map).get("input"));
        }
        setOutput();
        conf.set(CubertStrings.JSON_OUTPUT, root.get("output").toString());
        if (root.has("metadata"))
            conf.set(CubertStrings.JSON_METADATA, root.get("metadata").toString());

        conf.set(CubertStrings.JSON_MAP_OPERATOR_LIST, root.get("map").toString());
        job.setMapperClass(CubertMapper.class);

        if (root.has("shuffle") && !root.get("shuffle").isNull())
        {
            setShuffle();
            conf.set(CubertStrings.JSON_SHUFFLE, root.get("shuffle").toString());

            conf.set(CubertStrings.JSON_REDUCE_OPERATOR_LIST, root.get("reduce")
                                                                  .toString());
            job.setReducerClass(CubertReducer.class);

        }

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

    private void processJobCommands(ArrayNode commands)
    {
        for (int i = 0; i < commands.size(); i++)
        {
            execJobCommand(commands.get(i));
        }

    }

    private void execJobCommand(JsonNode jsonNode)
    {

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

        for (JsonNode node : asArray(root, "libjars"))
        {
            Path path = new Path(node.getTextValue());

            if (localFs.exists(path))
            {
                Path dstPath = new Path(tmpDir, path.getName());
                fs.copyFromLocalFile(path, dstPath);

                path = dstPath;
            }

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
            Path path = new Path(cachedFile.getTextValue());
            print.f("CACHING file %s", path);

            String pathInString = path.toString();
            String fragment = null;
            if (pathInString.contains("#"))
            {
                fragment = pathInString.substring(pathInString.indexOf("#"));
                pathInString = pathInString.substring(0, pathInString.indexOf("#"));
            }

            FileStatus status = fs.getFileStatus(new Path(pathInString));
            if (status.isDir())
            {
                FileStatus[] children = fs.globStatus(new Path(path + "/*"));
                for (FileStatus child : children)
                {
                    if (child.isDir())
                        continue;
                    String name = child.getPath().getName();
                    if (name.startsWith("_") || name.startsWith("."))
                        continue;
                    if (!fs.exists(child.getPath()))
                        throw new IOException("File [" + child.getPath() + "] not found.");
                    DistributedCache.addCacheFile(child.getPath().toUri(), conf);
                    break;
                }
            }
            else
            {
                URI uri = new Path(pathInString).toUri();
                String uriEncoded = uri.toString();
                if (fragment != null)
                {
                    uriEncoded = uriEncoded + fragment;
                }

                path = new Path(uri);
                if (!fs.exists(path))
                    throw new IOException("File [" + path + "] not found.");
                DistributedCache.addCacheFile(new URI(uriEncoded), conf);
            }
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

        for (JsonNode indexNode : root.path("cacheIndex"))
        {
            // extract the index named by "index" from the location specified in "path";
            Index indexToCache =
                    Index.extractFromRelation(conf, getText(indexNode, "path"));

            String indexName = JsonUtils.getText(indexNode, "name");

            Path indexPath = new Path(tmpDir, UUID.randomUUID().toString());
            SerializerUtils.serializeToFile(conf, indexPath, indexToCache);

            DistributedCache.addCacheFile(new URI(indexPath.toString()), conf);

            // tmpFiles.add(indexPath);

            conf.set(CubertStrings.JSON_CACHE_INDEX_PREFIX + indexName,
                     indexPath.getName());

            print.f("Caching index at path [%s] as [%s]",
                    getText(indexNode, "path"),
                    indexPath.toString());
        }

    }

    protected void setInput(JsonNode input) throws IOException,
            ClassNotFoundException
    {
        JsonNode params = input.get("params");
        if (params == null)
            params = mapper.createObjectNode();

        // RelationType type = RelationType.valueOf(getText(input, "type"));
        List<Path> paths = FileSystemUtils.getPaths(fs, input.get("path"));

        if (paths.isEmpty())
        {
            throw new IOException("No input paths are defined");
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
                System.err.println("CONFIG ERROR: property mapreduce.input.fileinputformat.split.maxsize is not set when using combined input format");
                System.exit(-1);
            }
        }

        // add input paths to the job
        FileInputFormat.setInputPaths(job, paths.toArray(new Path[] {}));

        confDiff.endDiff();
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
        job.setPartitionerClass(CubertPartitioner.class);

        JsonNode shuffle = get(root, "shuffle");
        Storage storage = StorageFactory.get(getText(shuffle, "type"));
        storage.prepareOutput(job, conf, null, null, null);

        if (shuffle.has("aggregates"))
        {
            job.setCombinerClass(CubertCombiner.class);
        }
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

        if (root.has("shuffle") && !root.get("shuffle").isNull())
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
