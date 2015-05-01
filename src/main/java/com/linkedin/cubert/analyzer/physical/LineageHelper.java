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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.analyzer.physical.SemanticAnalyzer;
import com.linkedin.cubert.analyzer.physical.Lineage.*;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.aggregate.AggregationType;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.Pair;
import com.linkedin.cubert.utils.RewriteUtils;

public class LineageHelper implements OperatorVisitor
{
    private static final boolean TRACE_ON = false;

    public static void trace(String traceMsg)
    {
        if (TRACE_ON)
            System.out.println(traceMsg);
    }

    private int curSequence = 0;

    public ArrayList<ObjectNode> operatorList = new ArrayList<ObjectNode>();
    public HashMap<Integer, Pair<ObjectNode, JsonNode>> operatorPhaseMap =
            new HashMap<Integer, Pair<ObjectNode, JsonNode>>();

    public HashMap<Integer, List<String>> loadPathsMap =
            new HashMap<Integer, List<String>>();
    public HashMap<Integer, List<String>> storePathsMap =
            new HashMap<Integer, List<String>>();
    public HashMap<Integer, String> outputRelations = new HashMap<Integer, String>();

    public boolean inspect(ObjectNode jobNode, JsonNode phaseNode, ObjectNode operatorNode)
    {

        int opSequence = operatorList.size();

        operatorPhaseMap.put(opSequence, new Pair(jobNode, phaseNode));

        operatorList.add(operatorNode);

        curSequence++;

        if (isLoadOperator(jobNode, phaseNode, operatorNode))
        {
            List<String> loadPaths = getPaths(operatorNode.get("path"));
            // trace("Load operator visited " + operatorNode.toString());
            operatorMapPut(loadPathsMap, operatorNode, loadPaths);
        }

        if (isStoreCommand(jobNode, phaseNode, operatorNode))
        {
            List<String> storePaths = getPaths(operatorNode.get("path"));
            operatorMapPut(storePathsMap, operatorNode, storePaths);
            // trace("Pre Lineage Visitor visited store command = " +
            // operatorNode.toString());
        }

        operatorMapPut(outputRelations,
                       operatorNode,
                       getOperatorOutput(jobNode, phaseNode, operatorNode));
        return true;
    }

    public <T> void operatorMapPut(HashMap<Integer, T> operatorMap,
                                   ObjectNode operatorNode,
                                   T valueObj)
    {
        Integer opSequence = this.getOpSequence(operatorNode);
        operatorMap.put(opSequence, valueObj);

    }

    public <T> T operatorMapGet(HashMap<Integer, T> operatorMap, ObjectNode operatorNode)
    {
        Integer opSequence = this.getOpSequence(operatorNode);
        return operatorMap.get(opSequence);
    }

    public List<ObjectNode> findAllParentStores(ObjectNode jobNode,
                                                JsonNode phaseNode,
                                                ObjectNode opNode)
    {
        if (!isLoadOperator(jobNode, phaseNode, opNode))
            throw new RuntimeException("Cannot find parent store for non-LOAD "
                    + opNode.toString());

        List<ObjectNode> result = new ArrayList<ObjectNode>();
        List<String> loadPaths = operatorMapGet(loadPathsMap, opNode);
        for (String loadPath : loadPaths)
        {
            ObjectNode storeNode = findPrecedingStore(opNode, loadPath);
            if (storeNode != null)
                result.add(storeNode);
        }
        return result;
    }

    public ArrayList<ObjectNode> findAllOperatorSources(ObjectNode jobNode,
                                                        JsonNode phaseNode,
                                                        ObjectNode opNode)
    {
        ArrayList<ObjectNode> sourceNodes = new ArrayList<ObjectNode>();

        if (isLoadOperator(jobNode, phaseNode, opNode))
        {
            List<String> loadPaths = operatorMapGet(loadPathsMap, opNode);
            for (String loadPath : loadPaths)
            {
                ObjectNode storeNode = findPrecedingStore(opNode, loadPath);
                if (storeNode != null)
                    sourceNodes.add(storeNode);
            }
            return sourceNodes;
        }

        JsonNode inputsNode =
                (isStoreCommand(jobNode, phaseNode, opNode) ? opNode.get("name")
                        : opNode.get("input"));
        if (inputsNode == null)
        {
            // trace("Getting sources for " + opNode.toString() + " ?");
            return null;
        }

        if (!(inputsNode instanceof ArrayNode))
            sourceNodes.addAll(findOperatorInputSources(opNode, inputsNode.getTextValue()));
        else
        {
            for (JsonNode inputNode : (ArrayNode) inputsNode)
                sourceNodes.addAll(findOperatorInputSources(opNode,
                                                            inputNode.getTextValue()));
        }

        return sourceNodes;
    }

    public ObjectNode findPrecedingStore(ObjectNode opNode, String loadPath)
    {
        int opSequence = this.getOpSequence(opNode);
        // trace("findPrecedingStore called for opNode " + opNode +
        // " loadPath= " + loadPath);
        for (int i = opSequence; i >= 0; i--)
        {
            ObjectNode candidateOp = operatorList.get(i);
            Pair<ObjectNode, JsonNode> phaseInfo = getJobPhase(candidateOp);
            if (!isStoreCommand(phaseInfo.getFirst(), phaseInfo.getSecond(), candidateOp))
                continue;

            // trace("Examining store command " + candidateOp);
            List<String> storePaths = operatorMapGet(storePathsMap, candidateOp);
            if (storePaths.indexOf(loadPath) == -1)
                continue;
            return candidateOp;
        }
        return null;
    }

    public ObjectNode findOperatorSource(ObjectNode opNode, String inputRelation)
    {
        int opSequence = this.getOpSequence(opNode);
        JsonNode phaseNode = this.getJobPhase(opNode).getSecond();
        ObjectNode sourceOp = findOperatorSourcePrior(opSequence - 1, inputRelation);
        if (sourceOp == null)
            throw new RuntimeException("Cannot find source for opNode "
                    + opNode.toString() + "for relation " + inputRelation);
        return sourceOp;
    }

    // Find the most recent operator source from a list of operators, given
    // an input relation.
    private ObjectNode getOperatorSourceInPhase(ObjectNode jobNode,
                                                JsonNode phaseNode,
                                                ObjectNode opNode,
                                                String inputRelation)
    {
        ArrayNode opsNode =
                isReducePhase(phaseNode) ? (ArrayNode) phaseNode
                        : (ArrayNode) (((ObjectNode) phaseNode).get("operators"));

        boolean reachedDest = false;

        // don't expect to find store in the list of oeprators.
        if (isReducePhase(phaseNode) && isStoreCommand(jobNode, phaseNode, opNode))
            reachedDest = true;

        for (int i = opsNode.size() - 1; i >= 0; i--)
        {
            ObjectNode candidateOp = (ObjectNode) (opsNode.get(i));
            if (candidateOp == opNode)
            {
                reachedDest = true;
                continue;
            }

            if (!reachedDest)
                continue;
            if (isOutputOf(candidateOp, inputRelation))
                return candidateOp;
        }
        return null;
    }

    public List<ObjectNode> findOperatorInputSources(ObjectNode opNode,
                                                     String inputRelation)
    {
        int opSequence = this.getOpSequence(opNode);
        ObjectNode jobNode = (ObjectNode) (this.getJobPhase(opNode).getFirst());
        JsonNode phaseNode = this.getJobPhase(opNode).getSecond();
        List<ObjectNode> result = new ArrayList<ObjectNode>();

        if (isReducePhase(phaseNode)
                && (opSequence == 0 || getOperatorSourceInPhase(jobNode,
                                                                (ArrayNode) phaseNode,
                                                                opNode,
                                                                inputRelation) == null))
        {
            // if either first operator in reduce phase or a matching source within
            // the same phase cannot be found,
            // look inside all the map jobs.
            ArrayNode mapsArray = (ArrayNode) getJobPhase(opNode).getFirst().get("map");
            for (JsonNode mapNode : mapsArray)
            {
                ArrayNode mapOps = (ArrayNode) ((ObjectNode) mapNode).get("operators");
                if (mapOps == null || mapOps.size() == 0)
                    continue;
                ObjectNode lastOp = (ObjectNode) mapOps.get(mapOps.size() - 1);
                result.add(findOperatorSourcePrior(getOpSequence(lastOp), inputRelation));
            }
        }
        else
            result.add(findOperatorSource(opNode, inputRelation));

        return result;

    }

    public ObjectNode findOperatorSourcePrior(int startSequence, String inputRelation)
    {
        for (int i = startSequence; i >= 0; i--)
        {
            ObjectNode candidateOp = operatorList.get(i);

            if (isOutputOf(candidateOp, inputRelation))
                return candidateOp;
        }
        return null;
    }

    public boolean isOutputOf(ObjectNode candidateOp, String inputRelation)
    {
        Pair<ObjectNode, JsonNode> jobPhase = this.getJobPhase(candidateOp);
        JsonNode phaseNode = jobPhase.getSecond();
        String outRelation =
                getOperatorOutput(jobPhase.getFirst(), phaseNode, candidateOp);
        if (outRelation != null && outRelation.equals(inputRelation))
            return true;

        // if this is an INPUT load, then match against "input"
        if (!isReducePhase(phaseNode) && phaseNode.get("input") == candidateOp
                && candidateOp.get("name").getTextValue().equals(inputRelation))
            return true;
        return false;
    }

    public Pair<ObjectNode, JsonNode> getJobPhase(ObjectNode opNode)
    {
        return operatorMapGet(operatorPhaseMap, (opNode));
    }

    public int getOpSequence(ObjectNode opNode)
    {
        int result = CommonUtils.indexOfByRef(this.operatorList, opNode);
        if (result == -1)
            throw new RuntimeException("No operatorList reference found \n opNode = "
                    + opNode.toString());
        return result;
    }

    public static boolean isLoadOperator(ObjectNode jobNode,
                                         JsonNode phaseNode,
                                         ObjectNode operatorNode)
    {
        if ((operatorNode.get("operator") == null && !isReducePhase(phaseNode) && ((ObjectNode) (phaseNode.get("input"))) == operatorNode)
                || operatorNode.get("operator") != null
                && operatorNode.get("operator")
                               .getTextValue()
                               .equalsIgnoreCase("LOAD_BLOCK"))
            return true;

        // LineageHelper.trace("operator-txt= " + operatorNode.get("operator") +
        // " phase type= " + (isReducePhase(phaseNode) ? "reduce" : "Map") +
        // " ,inputToPhase = " + (phaseNode.get("input") == operatorNode ? "true" :
        // "false" ));

        return false;
    }

    public static boolean isReducePhase(JsonNode phaseNode)
    {
        return phaseNode instanceof ArrayNode;
    }

    public static boolean isStoreCommand(ObjectNode jobNode,
                                         JsonNode phaseNode,
                                         ObjectNode opNode)
    {
        return (jobNode.get("output") == opNode || opNode.get("operator") != null
                && opNode.get("operator").getTextValue().equals("TEE") ? true : false);
    }

    public static String getPathRoot(JsonNode pathNode)
    {
        if (pathNode instanceof ObjectNode
                && ((ObjectNode) pathNode).get("startDate") != null)
            return (pathNode.get("root").getTextValue());
        else
            return (pathNode.getTextValue());

    }

    public static List<String> getPaths(JsonNode pathNode)
    {
        List<String> resultPaths = new ArrayList<String>();

        if (!(pathNode instanceof ArrayNode))
        {
            resultPaths.add(getPathRoot(pathNode));
            return resultPaths;
        }

        ArrayNode pathArray = (ArrayNode) pathNode;
        for (JsonNode pathElement : pathArray)
        {
            resultPaths.add(getPathRoot(pathElement));
        }

        return resultPaths;
    }

    public static String getOperatorOutput(ObjectNode jobNode,
                                           JsonNode phaseNode,
                                           ObjectNode operatorNode)
    {
        if (operatorNode.get("output") != null)
        {
            return operatorNode.get("output").getTextValue();
        }
        if (LineageHelper.isStoreCommand(jobNode, phaseNode, operatorNode))
            return operatorNode.get("name").getTextValue();
        return null;
    }
}
