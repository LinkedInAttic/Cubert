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

package com.linkedin.cubert.analyzer.physical;

import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.io.Storage;
import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.utils.FileSystemUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.print;

/**
 * Analyzes the data dependency across the jobs in the program.
 * 
 * Rewrites the plan to add following fields in the plan:
 * 
 * @author Maneesh Varshney
 * 
 */
public class DependencyAnalyzer extends PhysicalPlanVisitor implements PlanRewriter
{

    /**
     * Input and outputs for a job.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class JobInputsOutputs
    {
        private final Set<String> inputs = new HashSet<String>();
        private final Set<String> outputs = new HashSet<String>();
        private final Map<String, String> typeMap = new HashMap<String, String>();
        private final Map<String, JsonNode> jsonMap = new HashMap<String, JsonNode>();

        void addInput(String type, String name, JsonNode json)
        {
            inputs.add(name);
            typeMap.put(name, type);
            jsonMap.put(name, json);
        }

        void addOutput(String type, String name, JsonNode json)
        {
            outputs.add(name);
            typeMap.put(name, type);
            jsonMap.put(name, json);
        }
    }
    private final List<JobInputsOutputs> jobDependency =
            new ArrayList<JobInputsOutputs>();
    private JobInputsOutputs currentJob;
    private final Configuration conf = new JobConf();

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        new PhysicalPlanWalker(plan, this).walk();
        return plan;
    }

    @Override
    public void enterJob(JsonNode json)
    {
        currentJob = new JobInputsOutputs();
    }

    @Override
    public void visitInput(JsonNode json)
    {
        String type = getText(json, "type");
        for (JsonNode pathJson : json.path("path"))
        {
            currentJob.addInput(type, JsonUtils.encodePath(pathJson), json);
        }

    }

    @Override
    public void visitOperator(JsonNode json, boolean isMapper)
    {
        String type = getText(json, "operator");

        if (type.equals("LOAD_CACHED_FILE"))
        {
            currentJob.addInput(getText(json, "type"), getText(json, "path"), json);
        }
        if (type.equals("TEE"))
        {
            currentJob.addOutput(getText(json, "type"), getText(json, "path"), json);
        }
    }

    @Override
    public void visitOutput(JsonNode json)
    {
        currentJob.addOutput(getText(json, "type"), getText(json, "path"), json);
    }

    @Override
    public void visitCachedIndex(JsonNode json)
    {
        currentJob.addInput("RUBIX", getText(json, "path"), json);
    }

    @Override
    public void exitJob(JsonNode json)
    {
        jobDependency.add(currentJob);
    }

    @Override
    public void exitProgram(JsonNode json)
    {
        ObjectMapper mapper = new ObjectMapper();

        // Create reverse dependencies.
        // jobInputMap: From file name => jobId that needs this data as input
        // jobOutputMap: From file name => jobId that generates this data as output
        Map<String, List<Integer>> jobInputMap = new HashMap<String, List<Integer>>();
        Map<String, List<Integer>> jobOutputMap = new HashMap<String, List<Integer>>();

        int jobId = 0;
        for (JobInputsOutputs dep : jobDependency)
        {
            createReverseDependency(jobId, dep.inputs, jobInputMap);
            createReverseDependency(jobId, dep.outputs, jobOutputMap);
            jobId++;
        }

        // Infer data dependency across the jobs.
        // Strategy: look at inputs for a given job. If the input is generated by some
        // other job, then we have a dependency.
        // If the input is not generated by any other job, then this input is a "global"
        // input to the program (which we will handle later in this function).
        Set<String> programInputs = new HashSet<String>();
        jobId = 0;
        for (JobInputsOutputs dep : jobDependency)
        {
            // upstreamJob: all jobs that generate data that this job needs as input
            Set<Integer> upstreamJobs = new HashSet<Integer>();

            for (String input : dep.inputs)
            {
                // upstreamJobsForInput: all jobs that generate this particular input
                List<Integer> upstreamJobsForInput = jobOutputMap.get(input);
                if (upstreamJobsForInput == null)
                {
                    programInputs.add(input);
                }
                else
                {
                    upstreamJobs.addAll(upstreamJobsForInput);
                }
            }

            // Add the inferred dependency in the json.
            // The json syntax is {"dependsOn": [job_id, job_id...]}
            ArrayNode arrayNode = mapper.createArrayNode();
            for (Integer job : upstreamJobs)
                arrayNode.add((int) job);

            ((ObjectNode) json.get("jobs").get(jobId)).put("dependsOn", arrayNode);

            jobId++;
        }

        // Now handle the "program inputs". These are data files that are not generated by
        // any job, that is, they are input to the program.

        // First, merge the typeMap (and jsonMap) from all jobDependency.
        Map<String, String> typeMap = new HashMap<String, String>();
        Map<String, JsonNode> jsonMap = new HashMap<String, JsonNode>();

        for (JobInputsOutputs dep : jobDependency)
        {
            for (String input : dep.typeMap.keySet())
            {
                String type = dep.typeMap.get(input);
                if (typeMap.containsKey(input) && !typeMap.get(input).equals(type))
                {
                    throw new IllegalArgumentException(String.format("Dataset [%s] is used within the program with different types: %s and %s",
                                                                     input,
                                                                     type,
                                                                     typeMap.get(input)));

                }
                typeMap.put(input, type);
                jsonMap.put(input, dep.jsonMap.get(input));
            }
        }
        print.f("[Dependency Analyzer] Program inputs: %s", programInputs);
        // Next, we obtain the schema of the program input files,
        // and put them in the json
        ObjectNode programInputsJson = mapper.createObjectNode();
        ((ObjectNode) json).put("input", programInputsJson);
        for (String input : programInputs)
        {
            String type = typeMap.get(input);
            JsonNode inputJson = jsonMap.get(input);
            try
            {
                PostCondition condition = getPostCondition(input, inputJson, type);
                ObjectNode node = mapper.createObjectNode();
                node.put("type", type);
                node.put("schema", condition.getSchema().toJson());
                if (condition.getPartitionKeys() != null)
                    node.put("partitionKeys",
                             JsonUtils.createArrayNode(condition.getPartitionKeys()));
                if (condition.getSortKeys() != null)
                    node.put("sortKeys",
                             JsonUtils.createArrayNode(condition.getSortKeys()));

                programInputsJson.put(input, node);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private void createReverseDependency(int jobId,
                                         Set<String> datasets,
                                         Map<String, List<Integer>> map)
    {
        for (String input : datasets)
        {
            List<Integer> jobs = map.get(input);
            if (jobs == null)
            {
                jobs = new ArrayList<Integer>();
                map.put(input, jobs);
            }
            jobs.add(jobId);
        }
    }

    private PostCondition getPostCondition(String input, JsonNode json, String typeStr) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);

        JsonNode pathJson = JsonUtils.decodePath(input);
        Path path = null;

        List<Path> paths = FileSystemUtils.getPaths(fs, pathJson);
        path = paths.get(0);

        Storage storage = StorageFactory.get(typeStr);
        return storage.getPostCondition(conf, json, path);

    }
}
