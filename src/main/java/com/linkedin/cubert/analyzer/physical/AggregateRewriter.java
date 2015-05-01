/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

package com.linkedin.cubert.analyzer.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import com.linkedin.cubert.analyzer.physical.SemanticAnalyzer.Node;

import com.linkedin.cubert.operator.aggregate.AggregationType;
import com.linkedin.cubert.analyzer.physical.Lineage;

import com.linkedin.cubert.analyzer.physical.LineageGraph.LineageGraphVertex;
import com.linkedin.cubert.analyzer.physical.LineageGraph.LineagePath;
import com.linkedin.cubert.analyzer.physical.Lineage.LineageException;
import com.linkedin.cubert.analyzer.physical.Lineage.OperatorVisitor;
import com.linkedin.cubert.analyzer.physical.Lineage.OutputColumn;
import com.linkedin.cubert.analyzer.physical.OperatorLineage;
import com.linkedin.cubert.block.BlockSchema;

import com.linkedin.cubert.utils.AvroUtils;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.DateTimeUtilities;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.Pair;
import com.linkedin.cubert.utils.RewriteUtils;
import com.linkedin.cubert.plan.physical.JobExecutor;
import com.linkedin.cubert.utils.CubertMD;

public abstract class AggregateRewriter
{
    public abstract void rewriteFactBlockgenPath(ObjectNode cubeNode,
                                                 LineagePath opSequencePath,
                                                 ObjectNode factNode,
                                                 Pair<ObjectNode, ObjectNode> bgInfo) throws AggregateRewriteException;

    public abstract void transformFactPreBlockgen(ObjectNode programNode,
                                                  Pair<ObjectNode, ObjectNode> bgInfo) throws AggregateRewriteException;

    public abstract ObjectNode transformMVPreCombine(String mvName);

    public abstract ObjectNode postCombineGroupBy();

    public abstract ObjectNode postCombineFilter();

    public abstract boolean isRewritable(ObjectNode operatorNode);

    public abstract ObjectNode preCubeTransform();

    public abstract String[] getMeasureColumns(ObjectNode cubeOperatorNode);

    public class AggregateRewriteException extends Exception
    {
        public AggregateRewriteException(String msg)
        {
            super(msg);
        }
    }

    public AggregateRewriter()
    {
        this.lineage = new Lineage();
    }

    /*
     * The main control flow for the rewrite is encapsulated in the base class. a cube or
     * group by operator which is a candidate for rewrite is identified and a rewrite
     * initiated if found.
     */
    public JsonNode rewrite(JsonNode plan)
    {
        JsonNode clonedPlan = JsonUtils.cloneNode(plan);
        AggregateRewriterVisitor sVisitor =
                new AggregateRewriterVisitor((ObjectNode) clonedPlan, this);

        Lineage.visitOperators((ObjectNode) clonedPlan, null, sVisitor);

        // if rewrite failed return the original plan.
        if (sVisitor.rewriteFailed)
            return plan;

        // return the modified plan
        return clonedPlan;

    }

    private static class AggregateRewriterVisitor implements Lineage.OperatorVisitor
    {
        private AggregateRewriter srewriter = null;
        private ObjectNode programNode = null;
        private boolean rewriteFailed = false;

        public AggregateRewriterVisitor(ObjectNode programNode,
                                        AggregateRewriter srewriter)
        {
            this.srewriter = srewriter;
            this.programNode = programNode;
        }

        @Override
        public boolean inspect(ObjectNode jobNode,
                               JsonNode phaseNode,
                               ObjectNode operatorNode)
        {
            if (operatorNode.get("operator") == null)
                return true;

            if (srewriter.isRewritable(operatorNode)
                    && operatorNode.get("summaryRewrite") != null)
            {
                try
                {
                    srewriter.injectRewrite(programNode,
                                            jobNode,
                                            phaseNode,
                                            operatorNode,
                                            operatorNode.get("mvName").getTextValue(),
                                            operatorNode.get("mvPath").getTextValue());

                }
                catch (AggregateRewriter.AggregateRewriteException e)
                {
                    System.out.println("Summary rewrite failed due to exception " + e);
                    this.rewriteFailed = true;
                    return false;
                }
                catch (IOException e)
                {
                    System.out.println("Summary rewrite failed due to IO exception " + e);
                    this.rewriteFailed = true;
                    return false;
                }
            }
            return true;
        }
    }

    protected String combinedRelation;
    protected String[] preCubeLoadColumns;

    protected static final String DATE_COLUMN_NAME = "__dateValue";
    protected HashMap<String, Integer> mvRefreshMap = new HashMap<String, Integer>();
    protected int factStartDate = 0;
    protected int factEndDate = 0;
    protected HashMap<Integer, List<OutputColumn>> measureLineageMap =
            new HashMap<Integer, List<OutputColumn>>();
    protected List<ObjectNode> factNodes = new ArrayList<ObjectNode>();
    protected ObjectNode preCubeFactLoad;
    protected HashSet<OutputColumn> factColumns = new HashSet<OutputColumn>();
    protected String factBgInput;
    protected String storedFactPathName;
    public boolean mvExists = true;
    protected String dateColumnAlias = null; // the column name assigned to date column
                                             // currently
    protected boolean formattedDateColumn = false; // a formatted date column is available
                                                   // in the critical path.
    protected ObjectNode tNode = null; // timespec node for current fact

    protected List<String> incFactTables = new ArrayList<String>(); // explicitly
                                                                    // identified fact
                                                                    // tables
    protected int mvRefreshDate = 0;
    protected int mvRefreshDateOverride = -1;
    protected DateTime incLoadDate = null;
    protected int mvHorizonDate;

    protected Lineage lineage;
    protected ObjectNode programNode;
    protected ObjectNode cubeJobNode;
    protected JsonNode cubePhaseNode;
    protected ObjectNode cubeOperatorNode;
    protected Pair<ObjectNode, ObjectNode> bgInfo = null;
    protected ArrayList<Pair<ObjectNode, ObjectNode>> bgInfoList =
            new ArrayList<Pair<ObjectNode, ObjectNode>>();
    protected ArrayList<ObjectNode> combineNodes = null;
    protected String mvName;
    protected String mvPath;

    public void injectRewrite(ObjectNode programNode,
                              ObjectNode jobNode,
                              JsonNode phaseNode,
                              ObjectNode cubeOperatorNode,
                              String mvName,
                              String mvPath) throws AggregateRewriteException,
            IOException
    {
        this.programNode = programNode;
        this.cubeJobNode = jobNode;
        this.cubePhaseNode = phaseNode;
        this.cubeOperatorNode = cubeOperatorNode;
        this.mvName = mvName;
        this.mvPath = mvPath;

        try
        {
            this.lineage.buildLineage(programNode);
        }
        catch (LineageException e)
        {
            throw new AggregateRewriteException(e.toString());
        }

        readSummaryMetaData();

        String[] measures = getMeasureColumns(cubeOperatorNode);
        String[] dimensions = getDimensionColumns(cubeOperatorNode);

        System.out.println("Measure columns = " + Arrays.toString(measures)
                + " Dimensions=" + Arrays.toString(dimensions));
        traceFactLoads(measures);
        traceFactBlockgens();
        calculateIncrementalFactLoadDates();
        rewriteFactBlockgenPaths();
        // Perform any pre-blockgen xforms needed on the fact.
        transformFactPreBlockgen(programNode, bgInfo);
        createMVBlockgen(dimensions, measures);
        insertPreCubeCombine((String[]) (ArrayUtils.addAll(dimensions, measures)));
        incrementalizeFactLoads();
        insertMVRefreshDateJobHook();
        rewritePreCubeMeasureJoins();
        programNode.put("summaryRewrite", "true");
    }

    /*
     * From information about the measure columns, trace the fact table loads.
     */
    protected void traceFactLoads(String[] measures) throws AggregateRewriteException
    {
        for (int measureIndex = 0; measureIndex < measures.length; measureIndex++)
        {
            List<Lineage.OutputColumn> inputColumns;
            try
            {
                ObjectNode inputNode =
                        lineage.getLineage()
                               .getPreLineage()
                               .findOperatorSource(cubeOperatorNode,
                                                   cubeOperatorNode.get("input")
                                                                   .getTextValue());
                inputColumns =
                        lineage.traceLoadColumn(programNode,
                                                new OutputColumn(inputNode,
                                                                 measures[measureIndex]));

            }
            catch (LineageException e1)
            {
                throw new AggregateRewriteException("Lineage exception when tracing measure column load"
                        + e1.toString());
            }

            if (inputColumns == null || inputColumns.size() == 0)
                throw new AggregateRewriteException("NULL or empty inputColumns");
            List<OutputColumn> inputFactColumns = filterFactTableColumns(inputColumns);

            if (inputFactColumns == null || inputFactColumns.size() == 0)
                throw new AggregateRewriteException("NULL or empty input fact columns");

            measureLineageMap.put(new Integer(measureIndex), inputFactColumns);
            for (OutputColumn factColumn : inputFactColumns)
            {

                factNodes.add(factColumn.opNode);
                factColumns.add(factColumn);
            }
        }

        if (factNodes.size() == 0)
            throw new AggregateRewriteException("Could not locate Fact tables");

        // Trace (factLoad, optional BIJ, CREATE BLOCK, CUBE) path from each fact table
        for (ObjectNode fnode : factNodes)
        {
            System.out.println("Discovered fact node " + fnode);
            validateDateRanges(fnode);
        }
    }

    /*
     * From the known array of fact nodes, trace the list of blockgen paths to all of the
     * fact tables prior to the cubing jobs.
     */
    private void traceFactBlockgens() throws AggregateRewriteException
    {
        ArrayList<String> nodeSequence1 = new ArrayList<String>();
        nodeSequence1.add("BLOCK_INDEX_JOIN");
        nodeSequence1.add("CREATE_BLOCK");

        ArrayList<String> nodeSequence2 = new ArrayList<String>();
        nodeSequence2.add("CREATE_BLOCK");

        for (int i = 0; i < factNodes.size(); i++)
        {
            ObjectNode factNode = factNodes.get(i);
            List<LineagePath> match1 =
                    lineage.traceMatchingPaths(factNode,
                                               nodeSequence1,
                                               cubeOperatorNode,
                                               true);
            List<LineagePath> match2 =
                    lineage.traceMatchingPaths(factNode,
                                               nodeSequence2,
                                               cubeOperatorNode,
                                               true);
            if (isEmptyList(match1) && isEmptyList(match2))
                throw new AggregateRewriteException("Found NO matching paths from LOAD fact -> CUBE");
            // if (!isEmptyList(match1) && !isEmptyList(match2) || match1.size() > 1)
            // throw new
            // AggregateRewriteException("Found multiple matching paths from LOAD fact -> CUBE");

            LineagePath matchingPath =
                    (!isEmptyList(match1) ? match1.get(0) : match2.get(0));

            Pair<ObjectNode, ObjectNode> bgInfo1 = extractFactBlockgenInfo(matchingPath);
            if (bgInfo1 == null)
                throw new AggregateRewriteException("Could not locate blockgen info for fact");
            if (bgInfo != null && !bgInfo1.equals(bgInfo))
                throw new AggregateRewriteException("Found inconsistent Blockgen information for fact");
            bgInfo = bgInfo1;

            bgInfoList.add(bgInfo);
        }
    }

    private void readSummaryMetaData() throws IOException,
            AggregateRewriteException
    {
        FileSystem fs = FileSystem.get(new JobConf());
        this.mvExists = false;
        FileStatus[] files = fs.globStatus(new Path(mvPath + "/avro/*.avro"));
        if (files != null && files.length > 0)
        {
            this.mvExists = true;
            processSummaryMetaData(mvPath);
        }
    }

    private void rewriteFactBlockgenPaths() throws AggregateRewriteException
    {
        for (int i = 0; i < factNodes.size(); i++)
        {
            // walk path from factLoad to blockgen, validate operators are expected
            // introduce projection for date column
            ObjectNode factNode = factNodes.get(i);

            LineagePath criticalPath =
                    lineage.tracePath(factNode, bgInfoList.get(i).getSecond());
            rewriteFactBlockgenPath(cubeOperatorNode,
                                    criticalPath,
                                    factNode,
                                    bgInfoList.get(i));
        }
    }

    private void createMVBlockgen(String[] dimensions, String[] measures) throws AggregateRewriteException
    {
        String[] mvDimensions = (dimensions);
        String[] mvMeasures = (measures);
        String[] mvColumns = (String[]) ArrayUtils.addAll(mvDimensions, mvMeasures);

        // Step1: At the very beginnning of the program, create a blockgen by index for
        // the
        // historical MV
        if (this.mvExists)
        {
            ObjectNode mvBlockgenJobNode =
                    (ObjectNode) createBlockgenForMV(programNode,
                                                     cubeOperatorNode,
                                                     bgInfo,
                                                     mvName,
                                                     mvPath,
                                                     mvColumns);

            ArrayNode jobsListNode =
                    JsonUtils.insertNodeListBefore((ArrayNode) (programNode.get("jobs")),
                                                   cubeJobNode,
                                                   Arrays.asList(new ObjectNode[] { mvBlockgenJobNode }));
            programNode.put("jobs", jobsListNode);
        }
    }

    private void insertPreCubeCombine(String[] mvColumns) throws AggregateRewriteException
    {
        getLoadFromCubingJob(programNode, cubeJobNode, bgInfo);
        BlockSchema mvSchema = new BlockSchema(this.preCubeFactLoad.get("schema"));
        /*
         * Prior to OLAP cube count distinct, introduce(a) LOAD historical MV, (b) Combine
         * with input fact relation and merge of MV with a TEE operator to store the
         * updated MV tuples.
         */

        combineNodes =
                createCombineWithHistorical(programNode,
                                            cubeJobNode,
                                            cubePhaseNode,
                                            cubeOperatorNode,
                                            bgInfo,
                                            mvName,
                                            mvPath,
                                            mvSchema.toJson(),
                                            mvColumns);
    }

    private void incrementalizeFactLoads() throws AggregateRewriteException
    {
        // Step2: Change date parameters for LOAD of Fact relation (i.e inputNode) to
        // LAST_MV_REFRESH_DATE, $endDate;
        try
        {
            incrementalizeInputLoad(programNode,
                                    this.factNodes,
                                    cubeOperatorNode,
                                    mvName,
                                    mvPath);
        }
        catch (IOException e)
        {
            throw new AggregateRewriteException("IO exception when trying to incrementalize input load "
                    + e);
        }
    }

    private void insertMVRefreshDateJobHook()
    {
        // MV refresh map is updated AFTER incrementalizeInputLoad is called.
        String refreshCmd =
                "METAFILE UPDATE " + mvPath + " " + "mv.refresh.time "
                        + this.mvRefreshMap.get(mvName) + " mv.refresh.time.override "
                        + this.mvRefreshMap.get(mvName) + " mv.horizon.time "
                        + this.factStartDate;

        ((ArrayNode) (cubeJobNode.get("postJobHooks"))).add(refreshCmd);
    }

    private void rewritePreCubeMeasureJoins() throws AggregateRewriteException
    {
        for (OutputColumn inputFactColumn : this.factColumns)
        {
            List<ObjectNode> measureJoins = null;
            try
            {
                measureJoins = lineage.traceColumnJoins(programNode, inputFactColumn);
            }
            catch (LineageException e)
            {
                throw new AggregateRewriteException("Lineage exception when tracing column joins for "
                        + inputFactColumn);
            }

            List<ObjectNode> allJoins =
                    lineage.computeJoinsInJob(programNode, cubeJobNode);

            JsonNode joinPhase =
                    validateMeasureJoins(measureJoins,
                                         cubeJobNode,
                                         allJoins,
                                         inputFactColumn.opNode);
            // the last operator in the COMBINE chain will be a TEE whose input relation
            // is the
            // combined MV
            String combinedRelationName =
                    combineNodes.get(combineNodes.size() - 1)
                                .get("output")
                                .getTextValue();
            ArrayList<ObjectNode> newMeasureJoins =
                    rewriteMeasureJoins(programNode,
                                        cubeOperatorNode,
                                        combinedRelationName,
                                        measureJoins,
                                        inputFactColumn.opNode);

            // Delete all current measure Joins.
            ArrayList<ObjectNode> joinOpList = new ArrayList<ObjectNode>();
            joinOpList.addAll(measureJoins);
            deleteOperatorNodes(programNode, joinOpList);

            ArrayNode finalOps = null;

            // Insert the new Measure joins right after the combine nodes..
            finalOps =
                    JsonUtils.insertNodeListAfter(lineage.getPhaseOperators(joinPhase),
                                                  combineNodes.get(combineNodes.size() - 1),
                                                  newMeasureJoins);

            lineage.setPhaseOperators(cubeJobNode, joinPhase, finalOps);
        }
    }

    private void getLoadFromCubingJob(ObjectNode programNode,
                                      ObjectNode jobNode,
                                      Pair<ObjectNode, ObjectNode> blockgenInfo) throws AggregateRewriteException
    {
        ObjectNode blockgenNode = blockgenInfo.getSecond();
        this.storedFactPathName =
                lineage.getBlockgenStorePath(programNode, blockgenInfo.getSecond());

        if (storedFactPathName == null)
            throw new AggregateRewriteException("Unknown or ambiguous output path name for fact table in OLAP_CUBE phase");

        this.preCubeFactLoad = lineage.getMatchingLoadInJob(jobNode, storedFactPathName);
        if (preCubeFactLoad == null)
            throw new AggregateRewriteException("Cannot find matching LOAD for "
                    + storedFactPathName + " in job " + jobNode);

    }

    protected ObjectNode getFactTimeSpecNode(ObjectNode factNode, ObjectNode cubeNode) throws AggregateRewriteException
    {
        List<String> paths = lineage.getPaths(factNode.get("path"));
        for (JsonNode timeSpecNode : (ArrayNode) (cubeNode.get("timeColumnSpec")))
        {
            String elementPath =
                    ((ObjectNode) timeSpecNode).get("factPath").getTextValue();
            if (paths.indexOf(elementPath) != -1)
            {
                tNode = (ObjectNode) timeSpecNode;
                return tNode;
            }
        }
        throw new AggregateRewriteException("No matching time column specification found for FACT load at "
                + factNode.toString());
    }

    private Pair<ObjectNode, ObjectNode> extractFactBlockgenInfo(LineagePath matchingPath)
    {
        List<LineageGraphVertex> nodes = matchingPath.nodes;
        int size = nodes.size();
        if (size == 3)
        {
            ObjectNode bijNode = ((OperatorLineage) (nodes.get(0))).node;
            return new Pair<ObjectNode, ObjectNode>(bijNode,
                                                    ((OperatorLineage) (nodes.get(1))).node);
        }
        else
        {
            return new Pair<ObjectNode, ObjectNode>(null,
                                                    ((OperatorLineage) (nodes.get(0))).node);
        }

    }

    private boolean isEmptyList(List<LineagePath> match1)
    {
        return (match1 == null || match1.size() == 0 ? true : false);
    }

    private String[] getDimensionColumns(ObjectNode cubeOperatorNode)
    {
        String operatorType = cubeOperatorNode.get("operator").getTextValue();
        if (operatorType.equalsIgnoreCase("CUBE"))
            return JsonUtils.asArray(cubeOperatorNode.get("dimensions"));
        else if (operatorType.equalsIgnoreCase("GROUP_BY"))
            return JsonUtils.asArray(cubeOperatorNode.get("groupBy"));
        return null;
    }

    private boolean isDatedPath(ArrayNode pathArray)
    {
        for (JsonNode pathNode : pathArray)
        {
            if (pathNode instanceof ObjectNode
                    && ((ObjectNode) pathNode).get("startDate") != null)
                return true;
        }
        return false;
    }

    // All base relations that load a column are present in "inputColumns".
    // we are testing the fact table based on dated path.
    private List<OutputColumn> filterFactTableColumns(List<OutputColumn> inputColumns)
    {
        List<OutputColumn> result = new ArrayList<OutputColumn>();
        for (OutputColumn candCol : inputColumns)
        {
            System.out.println("traceFactTableColumns: printing candCol = "
                    + candCol.toString());

            if (isDatedPath((ArrayNode) candCol.opNode.get("path")))
            {
                if (!isAnIncrementalCandidate(candCol.opNode.get("path")))
                    continue;
                result.add(candCol);
            }
        }
        return result;
    }

    private boolean isAnIncrementalCandidate(JsonNode pathsNode)
    {
        List<String> paths = LineageHelper.getPaths(pathsNode);
        for (String s : paths)
        {
            if (incFactTables != null && incFactTables.size() > 0
                    && incFactTables.indexOf(s) == -1)
                return false;
            String[] splits = s.split("/");
            String fname = splits[splits.length - 1];
            if (fname.startsWith("dim") || fname.startsWith("DIM"))
                return false;
        }
        return true;
    }

    private JsonNode validateMeasureJoins(List<ObjectNode> measureJoins,
                                          JsonNode jobNode,
                                          List<ObjectNode> allJoins,
                                          ObjectNode factNode) throws AggregateRewriteException
    {
        JsonNode joinPhaseResult = null;
        List<JsonNode> mapArray = JsonUtils.toArrayList((ArrayNode) (jobNode.get("map")));

        System.out.println("MeasureJoins = \n"
                + CommonUtils.listAsString(measureJoins, "\n"));
        // All joins on measure column must be in the COMBINE or OLAP CUBE phase.
        for (ObjectNode measureJoin : measureJoins)
        {
            JsonNode joinPhase = lineage.getPhase(measureJoin);
            if (joinPhaseResult != null && joinPhase != joinPhaseResult)
                throw new AggregateRewriteException("Measure Joins placed at incorrect place");
            joinPhaseResult = joinPhase;
        }

        // Check that every join which occurs in this phase is a measure Join.
        for (ObjectNode joinOp : allJoins)
        {
            boolean found = false;
            if (CommonUtils.indexOfByRef(measureJoins, joinOp) == -1)
            {

                System.out.println("Problematic join = " + joinOp.toString());
                throw new AggregateRewriteException("Unsupported Join operator in Rewrite phase");
            }
        }

        // ensure all joins are in map phase
        if (CommonUtils.indexOfByRef(mapArray, joinPhaseResult) == -1)
            throw new AggregateRewriteException("Measure Joins found in reduce phase");

        // walk all operators after preCubeFactLoad. expect to find all measure joins
        List<JsonNode> mapOps =
                JsonUtils.toArrayList((ArrayNode) joinPhaseResult.get("operators"));

        int matchedJoins = 0;
        int preCubeFactLoadIdx = -1, firstJoinIdx = -1, opIdx = 0;
        System.out.println("Map operators are " + CommonUtils.listAsString(mapOps, "\n"));
        for (JsonNode mapOp : mapOps)
        {
            if (preCubeFactLoadIdx != -1)
            {
                if (lineage.isJoinOperator((ObjectNode) mapOp))
                {
                    if (CommonUtils.indexOfByRef(measureJoins, (ObjectNode) mapOp) != -1)
                        matchedJoins++;
                    if (firstJoinIdx == -1)
                        firstJoinIdx = opIdx;
                }
                else if (firstJoinIdx != -1)
                    break;
                opIdx++;
            }
            if (mapOp == this.preCubeFactLoad)
                preCubeFactLoadIdx = opIdx;

        }

        // the sequence of measure joins has to follow the fact load.
        if (matchedJoins != measureJoins.size() || !checkOtherJoinConstraints())
            throw new AggregateRewriteException("Measure Joins not placed contiguously in map phase, matchedJoins = "
                    + matchedJoins
                    + " firstJoinIdx = "
                    + firstJoinIdx
                    + " preCubeFactLoadIdx = " + preCubeFactLoadIdx);
        ;

        return joinPhaseResult;

    }

    private boolean checkOtherJoinConstraints()
    {
        // TODO Auto-generated method stub
        return true;
    }

    private void deleteOperatorNodes(ObjectNode programNode, List<ObjectNode> opNodeList)
    {
        ArrayNode reduceOps = null;
        ArrayNode mapOps = null;
        for (ObjectNode operatorNode : opNodeList)
        {
            JsonNode phaseNode = lineage.getPhase(operatorNode);

            if (phaseNode instanceof ArrayNode)
            {
                JsonUtils.deleteFromArrayNode((ArrayNode) phaseNode, operatorNode);

            }
            else
            {
                ArrayNode operatorListNode = (ArrayNode) phaseNode.get("operators");
                JsonUtils.deleteFromArrayNode(operatorListNode, operatorNode);
            }

        }

    }

    private ArrayList<ObjectNode> rewriteMeasureJoins(ObjectNode programNode,
                                                      ObjectNode cubeOperatorNode,
                                                      String combinedRelationName,
                                                      List<ObjectNode> measureJoins,
                                                      ObjectNode factNode)
    {
        ArrayList<ObjectNode> newMeasureJoins = new ArrayList<ObjectNode>();
        String[] newInputs;

        int inputIndex = 0;
        for (ObjectNode measureJoin : measureJoins)
        {
            ObjectNode newJoin = (ObjectNode) (measureJoin);
            String replacedInput = null;
            newInputs = JsonUtils.asArray(measureJoin.get("input"));
            int replacedInputIndex =
                    lineage.getDescendantInputIndex(newInputs, measureJoin, factNode);
            replacedInput = newInputs[replacedInputIndex];
            newInputs[replacedInputIndex] = combinedRelationName;

            System.out.println(String.format("Replaced join-input %s with %s",
                                             replacedInput,
                                             combinedRelationName));
            newJoin.put("input", JsonUtils.createArrayNode(newInputs));
            if (newJoin.get("leftBlock").equals(replacedInput))
                newJoin.put("leftBlock", combinedRelationName);
            else
                newJoin.put("rightBlock", combinedRelationName);

            newMeasureJoins.add(newJoin);
        }

        return newMeasureJoins;
    }

    private ArrayList<ObjectNode> createCombineWithHistorical(ObjectNode programNode,
                                                              ObjectNode jobNode,
                                                              JsonNode phaseNode,
                                                              ObjectNode cubeOperatorNode,
                                                              Pair<ObjectNode, ObjectNode> blockgenInfo,
                                                              String mvName,
                                                              String mvPath,
                                                              JsonNode mvSchemaJson,
                                                              String[] mvColumns) throws AggregateRewriteException

    {
        ArrayNode cacheIndexNode = null;
        if (jobNode.get("cacheIndex") == null)
            jobNode.put("cacheIndex", JsonUtils.createArrayNode());
        cacheIndexNode = (ArrayNode) jobNode.get("cacheIndex");

        ArrayList<ObjectNode> combineOps = new ArrayList<ObjectNode>();
        String factName = preCubeFactLoad.get("output").getTextValue();
        combinedRelation = factName;

        if (this.mvExists)
        {

            // We are loading the MV blocks using the MV index.
            cacheIndexNode.add(RewriteUtils.createObjectNode("name", factName
                    + "MV_BLOCKGEN_INDEX", "path", mvPath + "/blockgen"));

            // XXX- omitted type definitions here.
            ObjectNode loadMvNode =
                    RewriteUtils.createObjectNode("operator",
                                                  "LOAD_BLOCK",
                                                  "input",
                                                  preCubeFactLoad.get("output"),
                                                  "output",
                                                  factName + "_MV",
                                                  "index",
                                                  factName + "MV_BLOCKGEN_INDEX",
                                                  "path",
                                                  mvPath + "/blockgen");

            combineOps.add(loadMvNode);
            preCubeLoadColumns = lineage.getSchemaOutputColumns(preCubeFactLoad);

            ObjectNode transformMVNode = transformMVPreCombine(factName + "_MV");
            if (transformMVNode != null)
                combineOps.add(transformMVNode);

            ArrayNode combineInput = JsonUtils.createArrayNode();
            combineInput.add(preCubeFactLoad.get("output").getTextValue());
            combineInput.add(factName + "_MV");

            ObjectNode combineNode =
                    RewriteUtils.createObjectNode("operator",
                                                  "COMBINE",
                                                  "input",
                                                  combineInput,
                                                  "output",
                                                  combinedRelation,
                                                  "pivotBy",
                                                  JsonUtils.createArrayNode(preCubeLoadColumns));
            combineOps.add(combineNode);

            ObjectNode postCombineGby = postCombineGroupBy();
            if (postCombineGby != null)
                combineOps.add(postCombineGby);
        }

        ObjectNode postCombineFilter = postCombineFilter();
        if (postCombineFilter != null)
            combineOps.add(postCombineFilter);

        ObjectNode storeMVNode = createMVStorageNode();
        combineOps.add(storeMVNode);

        // A pre cube transform can be provided to handle special xforms
        // for instance for time series.
        ObjectNode preCubeTransform = preCubeTransform();
        if (preCubeTransform != null)
            combineOps.add(preCubeTransform);

        JsonNode loadPhase = lineage.getPhase(preCubeFactLoad);
        ArrayNode phaseOps = lineage.getPhaseOperators(loadPhase);
        phaseOps = JsonUtils.insertNodeListAfter(phaseOps, preCubeFactLoad, combineOps);
        lineage.setPhaseOperators(jobNode, loadPhase, phaseOps);

        // add post job hook to rename existing (old MV) to avro_old
        cubeJobNode.put("postJobHooks", JsonUtils.createArrayNode());
        ArrayNode jobHooks = (ArrayNode) (jobNode.get("postJobHooks"));

        addCubeJobHooks(jobHooks);
        return combineOps;
    }

    private void incrementalizeInputLoad(ObjectNode programNode,
                                         List<ObjectNode> factNodes2,
                                         ObjectNode cubeOperatorNode,
                                         String mvName,
                                         String mvPath) throws IOException,
            AggregateRewriteException
    {
        for (ObjectNode inputFactNode : factNodes2)
            incrementalizeInputLoad(programNode,
                                    inputFactNode,
                                    cubeOperatorNode,
                                    mvName,
                                    mvPath);
    }

    // check that all fact table path loads are in the same date range.
    private void validateDateRanges(ObjectNode inputNode) throws AggregateRewriteException
    {
        // TODO : validate that the date range spans less than or equal to a month and
        // future support for more time span.
        ArrayNode paths = (ArrayNode) inputNode.get("path");
        for (JsonNode pathNode : paths)
        {
            if (!(pathNode instanceof ObjectNode))
                continue;
            int startDate =
                    Integer.parseInt(((ObjectNode) pathNode).get("startDate")
                                                            .getTextValue());
            if (this.factStartDate == 0)
            {
                this.factStartDate = startDate;
                System.out.println("Setting fact startDate to " + startDate);
            }
            else if (startDate != this.factStartDate)
                throw new AggregateRewriteException("Inconsistent fact start dates");
            int endDate =
                    Integer.parseInt(((ObjectNode) pathNode).get("endDate")
                                                            .getTextValue());
            if (this.factEndDate == 0)
                this.factEndDate = endDate;
            else if (endDate != this.factEndDate)
                throw new AggregateRewriteException("Inconsistent fact end dates");
        }
        // restricting date range to one Month.
        // TODO: how does the Rubix runtime know that computation is iterative, occurs
        // daily
        // and startDate incremented by 1 each day ?
        String es = Integer.toString(factEndDate);
        String ss = Integer.toString(factStartDate);
        if (DateTimeUtilities.getDateTime(es)
                             .minusDays(30)
                             .isAfter(DateTimeUtilities.getDateTime(ss)))
            throw new AggregateRewriteException(String.format("[sd=%d, ed=%d] Time spans larger than a month are currently not supported",
                                                              factStartDate,
                                                              factEndDate));

    }

    private void processSummaryMetaData(String mvPath) throws AggregateRewriteException
    {
        // TODO: retrieve the MV horizon
        HashMap<String, String> metaEntries;
        try
        {
            metaEntries = CubertMD.readMetafile(mvPath);
        }
        catch (IOException e)
        {
            throw new AggregateRewriteException("Cannot read Meta file for summary at "
                    + mvPath);
        }

        // if user explicitly indicated incremental candidates
        String incCandidates;
        if ((incCandidates = metaEntries.get("mv.incremental.candidates")) != null)
        {
            String[] incFactTablesArray = incCandidates.split(",");
            for (String s : incFactTablesArray)
                incFactTables.add(s);
        }

        if (metaEntries == null || metaEntries.get("mv.refresh.time") == null)
            throw new AggregateRewriteException("MV metaEntries not visible");
        mvHorizonDate = Integer.parseInt(metaEntries.get("mv.horizon.time"));
        mvRefreshDate = Integer.parseInt(metaEntries.get("mv.refresh.time"));
        if (metaEntries.get("mv.refresh.time.override") != null)
            mvRefreshDateOverride =
                    Integer.parseInt(metaEntries.get("mv.refresh.time.override"));
    }

    private void calculateIncrementalFactLoadDates() throws AggregateRewriteException
    {
        if (!this.mvExists)
            return;

        DateTime dt = null;
        /*
         * difference between factStartDate and mvHorizonDate determines #of bit shifts.
         * the new horizon date is always determined by the factStartDate.
         */
        if (this.factStartDate > mvHorizonDate)
            throw new AggregateRewriteException(String.format("Fact start date(%d) in the future of mv horizon date(%d) ",
                                                              factStartDate,
                                                              mvHorizonDate));

        dt = DateTimeUtilities.getDateTime(Integer.toString(mvRefreshDate));
        incLoadDate = dt.plusDays(1);

        // Handle over-ride case.
        if (mvRefreshDateOverride != -1)
        {
            // if over-ridden time is before the physical MV refresh time
            if (mvRefreshDateOverride != 0 && mvRefreshDateOverride < mvRefreshDate)
                incLoadDate =
                        DateTimeUtilities.getDateTime(Integer.toString(mvRefreshDateOverride))
                                         .plusDays(1);

            /*
             * mvRefreshDateOverride of 0 is treated as a hint to turn off
             * incrementalization.
             */
            else if (mvRefreshDateOverride == 0)
                incLoadDate = null;
        }

        if (mvRefreshDate != 0 && incLoadDate != null)
        {
            if (!(DateTimeUtilities.getDateTime(factStartDate).isBefore(incLoadDate) && DateTimeUtilities.getDateTime(factEndDate)
                                                                                                         .isAfter(incLoadDate)))
                throw new AggregateRewriteException(String.format("MV date range mis-matches load range[%s, %s] mvRefreshDate=%s ",
                                                                  factStartDate,
                                                                  factEndDate,
                                                                  mvRefreshDate));
        }

    }

    //
    private void incrementalizeInputLoad(ObjectNode programNode,
                                         ObjectNode inputNode,
                                         ObjectNode cubeOperatorNode,
                                         String mvName,
                                         String mvPath) throws IOException,
            AggregateRewriteException
    {

        // extract input paths from inputNode and adjust start-date to MV refresh date+1.
        ArrayNode paths = (ArrayNode) inputNode.get("path");
        System.out.println("Incrementalize InputNode = " + inputNode.toString());
        int newMvRefreshTime = 0;
        for (int i = 0; i < paths.size(); i++)
        {
            JsonNode pathNode = paths.get(i);
            if (pathNode instanceof ObjectNode)
            {
                String startDate =
                        ((ObjectNode) pathNode).get("startDate").getTextValue();
                // System.out.println("startDate = " + startDate);
                DateTime loadStart = DateTimeUtilities.getDateTime((startDate));
                String endDate = ((ObjectNode) pathNode).get("endDate").getTextValue();
                DateTime loadEnd = DateTimeUtilities.getDateTime(endDate);

                if (mvRefreshDate != 0 && incLoadDate != null)
                {
                    if (loadStart.isBefore(incLoadDate) && loadEnd.isAfter(incLoadDate))
                    {
                        ((ObjectNode) pathNode).put("origStartDate", startDate);
                        ((ObjectNode) pathNode).put("startDate",
                                                    Integer.toString(DateTimeUtilities.asInt(incLoadDate)));
                    }
                    else
                        throw new AggregateRewriteException(String.format("MV date range mis-matches load range[%s, %s] ",
                                                                          startDate,
                                                                          endDate));
                }

                newMvRefreshTime =
                        Math.max(Integer.parseInt(((ObjectNode) pathNode).get("endDate")
                                                                         .getTextValue()),
                                 newMvRefreshTime);
            }
        }

        System.out.println("Setting MV refresh time for " + mvName + " to "
                + newMvRefreshTime);
        mvRefreshMap.put(mvName, newMvRefreshTime);

    }

    private JsonNode createBlockgenForMV(ObjectNode programNode,
                                         ObjectNode cubeOperatorNode,
                                         Pair<ObjectNode, ObjectNode> bgInfo,
                                         String mvName,
                                         String mvPath,

                                         String[] mvColumns) throws AggregateRewriteException
    {
        String bgFactPath = null;

        String[] partitionKeys = null;
        String[] pivotKeys = null;
        String[] shufflePivotKeys = null;
        String mvInputPath = mvPath + "/avro";

        if (lineage.isBlockgenByIndex(bgInfo.getSecond()))
        {
            partitionKeys = JsonUtils.asArray(bgInfo.getFirst().get("partitionKeys"));

            String indexName = bgInfo.getFirst().get("index").getTextValue();
            ObjectNode jobNode = lineage.getOperatorJobNode(bgInfo.getSecond());

            // This should include BLOCK_ID, else assert.
            shufflePivotKeys =
                    JsonUtils.asArray(((ObjectNode) (jobNode.get("shuffle"))).get("pivotKeys"));
            String indexPath = lineage.traceIndexPath(jobNode, indexName);
            System.out.println("Traced blockgen index " + indexName + " path as"
                    + indexPath);
            System.out.println("job node = " + jobNode.toString());

            bgFactPath = indexPath;

        }
        else
        {
            bgFactPath =
                    lineage.getDatedPathRoot((ArrayNode) (bgInfo.getSecond().get("path")));
            partitionKeys = JsonUtils.asArray(bgInfo.getSecond().get("partitionKeys"));
            pivotKeys = JsonUtils.asArray(bgInfo.getSecond().get("pivotKeys"));
            shufflePivotKeys =
                    (String[]) ArrayUtils.addAll(new String[] { "BLOCK_ID" }, pivotKeys);
        }

        ArrayNode cacheIndexNode = JsonUtils.createArrayNode();
        cacheIndexNode.add(RewriteUtils.createObjectNode("name",
                                                         mvName + "_fact_index",
                                                         "path",
                                                         bgFactPath));
        JsonNode mapNode =
                JsonUtils.makeJson(String.format("[{'input' : {'name':'%s', 'type': 'AVRO', 'path':['%s']}}]",
                                                 mvName,
                                                 mvInputPath));

        System.out.println("Blockgen partition keys = " + Arrays.toString(partitionKeys));
        ObjectNode blockIndexJoin =
                RewriteUtils.createObjectNode("operator",
                                              "BLOCK_INDEX_JOIN",
                                              "input",
                                              mvName,
                                              "output",
                                              mvName,
                                              "index",
                                              mvName + "_fact_index",
                                              "partitionKeys",
                                              JsonUtils.createArrayNode(partitionKeys));

        ObjectNode mapperNode = (ObjectNode) (((ArrayNode) mapNode).get(0));
        mapperNode.put("operators", JsonUtils.createArrayNode());
        ((ArrayNode) (mapperNode.get("operators"))).add(blockIndexJoin);

        JsonNode shuffleNode =
                RewriteUtils.createObjectNode("name",
                                              mvName,
                                              "type",
                                              "SHUFFLE",
                                              "partitionKeys",
                                              JsonUtils.createArrayNode(new String[] { "BLOCK_ID" }),
                                              "pivotKeys",
                                              JsonUtils.createArrayNode(shufflePivotKeys));

        JsonNode reduceOpNode =
                RewriteUtils.createObjectNode("operator",
                                              "CREATE_BLOCK",
                                              "input",
                                              mvName,
                                              "output",
                                              mvName + "_blockgen",
                                              "blockgenType",
                                              "BY_INDEX",
                                              "index",
                                              mvName + "_fact_index",
                                              "indexPath",
                                              bgFactPath,
                                              "partitionKeys",
                                              JsonUtils.createArrayNode(new String[] { "BLOCK_ID" }),
                                              "pivotKeys",
                                              JsonUtils.createArrayNode(shufflePivotKeys),
                                              "originalPartitionKeys",
                                              JsonUtils.createArrayNode(partitionKeys));

        ArrayNode reduceNode = JsonUtils.createArrayNode();
        reduceNode.add(reduceOpNode);

        ObjectNode outputNode =
                RewriteUtils.createObjectNode("name",
                                              mvName + "_blockgen",
                                              "path",
                                              mvPath + "/blockgen",
                                              "type",
                                              "RUBIX",
                                              "params",
                                              RewriteUtils.createObjectNode("overwrite",
                                                                            "true"));

        ObjectNode jobNode =
                RewriteUtils.createObjectNode("name",
                                              "BLOCKGEN FOR MV",
                                              "map",
                                              mapNode,
                                              "shuffle",
                                              shuffleNode,
                                              "reduce",
                                              reduceNode,
                                              "output",
                                              outputNode);
        jobNode.put("cacheIndex", cacheIndexNode);
        jobNode.put("reducers", 100);
        System.out.println("JOB json = " + jobNode.toString());
        return jobNode;

    }

    /* default implementation provided. Derived class can over-ride */
    protected void addCubeJobHooks(ArrayNode jobHooks)
    {
        if (this.mvExists)
        {
            String renameCmd1 =
                    String.format("HDFS RENAME %s %s", mvPath + "/avro", mvPath
                            + "/avro_old");
            jobHooks.add(renameCmd1);
        }

        String renameCmd2 =
                String.format("HDFS RENAME %s %s", mvPath + "/avro_new", mvPath + "/avro");
        jobHooks.add(renameCmd2);
    }

    /*
     * Default implementation provided in the base class. A rewriter can over-ride as
     * needed
     */
    protected ObjectNode createMVStorageNode()
    {
        return RewriteUtils.createObjectNode("operator",
                                             "TEE",
                                             "input",
                                             combinedRelation,
                                             "output",
                                             combinedRelation,
                                             "type",
                                             "AVRO",
                                             "path",
                                             mvPath + "/avro_new",
                                             "passthrough", true);
    }

}
