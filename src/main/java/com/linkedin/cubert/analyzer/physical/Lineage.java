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
import com.linkedin.cubert.analyzer.physical.LineageGraph.*;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.aggregate.AggregationType;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.Pair;
import com.linkedin.cubert.utils.RewriteUtils;

public class Lineage
{
   

    public static class LineageException extends Exception
    {
        public LineageException(String mesg)
        {
            super(mesg);
        }
    }

    private static final int LEFT = 0;

    private static final int RIGHT = 0;

    private SemanticAnalyzer.Node nodeInformation;
    private LineageBuilder lineageInfo = null;
    private JsonNode programNode = null;

    public Lineage(SemanticAnalyzer.Node nodeInfo)
    {
        this.nodeInformation = nodeInfo;
    }

    public Lineage()
    {

    }

    public LineageBuilder getLineage()
    {
        return lineageInfo;
    }

    // 0423: cleanup starts here
    // A bunch of methods that walk operators.

   
    public String[] getSchemaOutputColumns(ObjectNode opNode)
    {

        BlockSchema bschema = new BlockSchema(opNode.get("schema"));
        return bschema.getColumnNames();

    }

    public boolean isBlockgenByIndex(ObjectNode bgNode)
    {
        // LineageHelper.trace("BGI check invoked for " + bgNode);
        String bgType = bgNode.get("blockgenType").getTextValue();
        return (bgType.equals("BY_INDEX") || bgType.equals("BY_BLOCK"));

    }


    public static String getDatedPathRoot(ArrayNode pathArray)
    {
        for (JsonNode pathNode : pathArray)
        {
            if (pathNode instanceof ObjectNode
                    && ((ObjectNode) pathNode.get("startDate") != null))
                return pathNode.get("root").getTextValue();
        }
        return null;
    }    

    public static List<String> getPaths(JsonNode pathNode)
    {
      return LineageHelper.getPaths(pathNode);
    }

    public static interface OperatorVisitor
    {
        public boolean inspect(ObjectNode jobNode,
                               JsonNode phaseNode,
                               ObjectNode operatorNode);
    }

    public static void visitOperators(ObjectNode programNode,
                                      ObjectNode jobNode,
                                      OperatorVisitor tracerObj)
    {
        visitOperators(programNode, jobNode, tracerObj, false);
    }

    private static int increment(boolean reverse)
    {
        return reverse ? -1 : 1;
    }

    public static void visitOperators(ObjectNode programNode,
                                      ObjectNode jobNode,
                                      OperatorVisitor tracerObj,
                                      boolean reverse)
    {
        ArrayNode jobs = (ArrayNode) programNode.get("jobs");
        int si = (reverse ? jobs.size() - 1 : 0);
        int ei = (reverse ? -1 : jobs.size());

        for (int i = si; i != ei; i = i + increment(reverse))
        {
            if (jobNode != null && jobNode != jobs.get(i))
                continue;

            if (!visitOperatorsInJob(programNode,
                                     (ObjectNode) jobs.get(i),
                                     tracerObj,
                                     reverse))
                return;
        }
    }

    private static boolean visitOperatorsInJob(ObjectNode programNode,
                                               ObjectNode jobNode,
                                               OperatorVisitor visitorObj,
                                               boolean reverse)
    {

        if (!visitMappers(jobNode, visitorObj, reverse))
            return false;

        if (!visitReducers(jobNode, visitorObj, reverse))
            return false;

        return true;
    }

    private static boolean visitReducers(ObjectNode jobNode,
                                         OperatorVisitor visitorObj,
                                         boolean reverse)
    {
        ArrayNode reduceOperators;
        if (jobNode.get("reduce") != null && !jobNode.get("reduce").isNull())
            reduceOperators = (ArrayNode) jobNode.get("reduce");
        else
            return true;

        if (reverse)
        {

            if (!visitorObj.inspect(jobNode,
                                    (JsonNode) reduceOperators,
                                    (ObjectNode) jobNode.get("output")))
                return false;
            if (!visitOperatorsInArray(jobNode,
                                       reduceOperators,
                                       reduceOperators,
                                       visitorObj,
                                       reverse))
                return false;
        }
        else
        {
            if (!visitOperatorsInArray(jobNode,
                                       reduceOperators,
                                       reduceOperators,
                                       visitorObj,
                                       reverse))
                return false;
            if (!visitorObj.inspect(jobNode,
                                    (JsonNode) reduceOperators,
                                    (ObjectNode) jobNode.get("output")))
                return false;
        }
        return true;
    }

    private static boolean visitMappers(ObjectNode jobNode,
                                        OperatorVisitor visitorObj,
                                        boolean reverse)
    {
        ArrayNode mappers = (ArrayNode) jobNode.get("map");
        int si = (reverse ? mappers.size() - 1 : 0);
        int ei = (reverse ? -1 : mappers.size());

        // TODO Auto-generated method stub
        for (int i = si; i != ei; i += increment(reverse))
        {
            if (!visitMapNode(jobNode, mappers.get(i), visitorObj, reverse))
                return false;
        }
        return true;
    }

    private static boolean visitMapNode(ObjectNode jobNode,
                                        JsonNode mapNode,
                                        OperatorVisitor visitorObj,
                                        boolean reverse)
    {
        if (reverse)
        {

            if (!visitOperatorsInArray(jobNode,
                                       mapNode,
                                       (ArrayNode) mapNode.get("operators"),
                                       visitorObj,
                                       reverse))
                return false;
            // LineageHelper.trace("Visiting map node input node reverse");
            if (!visitorObj.inspect(jobNode,
                                    (JsonNode) mapNode,
                                    (ObjectNode) mapNode.get("input")))
                return false;
        }
        else
        {
            // LineageHelper.trace("Visiting mapNode input forward");
            if (!visitorObj.inspect(jobNode,
                                    (JsonNode) mapNode,
                                    (ObjectNode) mapNode.get("input")))
                return false;

            if (!visitOperatorsInArray(jobNode,
                                       mapNode,
                                       (ArrayNode) mapNode.get("operators"),
                                       visitorObj,
                                       reverse))
                return false;
        }
        return true;
    }

    private static boolean visitOperatorsInArray(ObjectNode jobNode,
                                                 JsonNode phaseNode,
                                                 ArrayNode operatorArray,
                                                 OperatorVisitor visitorObj,
                                                 boolean reverse)
    {
        int si = reverse ? operatorArray.size() - 1 : 0;
        int ei = reverse ? -1 : operatorArray.size();
        for (int i = si; i != ei; i += increment(reverse))
        {
            if (!visitorObj.inspect(jobNode, phaseNode, (ObjectNode) operatorArray.get(i)))
                return false;
        }
        return true;
    }

   

   

    private static boolean isAvroLoad(ObjectNode jobNode,
                                      JsonNode phaseNode,
                                      ObjectNode operatorNode)
    {
        if (operatorNode.get("operator") != null
                || !operatorNode.get("type").getTextValue().equalsIgnoreCase("AVRO"))
            return false;
        return true;

    }

    // captures the notion of an output column which is a (opNode, columnName)
    public static class OutputColumn
    {
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
            result =
                    prime * result
                            + (opNode == null ? 0 : System.identityHashCode(opNode));
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            OutputColumn other = (OutputColumn) obj;
            if (columnName == null)
            {
                if (other.columnName != null)
                    return false;
            }
            else if (!columnName.equals(other.columnName))
                return false;
            if (opNode == null)
            {
                if (other.opNode != null)
                    return false;
            }
            else if (opNode != other.opNode)
                return false;
            return true;
        }

        public OutputColumn(ObjectNode opNode, String columnName)
        {
            this.opNode = opNode;
            this.columnName = columnName;
        }

        public String toString()
        {
            return ("opNode=" + opNode.toString() + " columnName = " + columnName.toString());
        }

        public ObjectNode opNode;
        public String columnName;
    }

    // Given a column referenced in the output of the specified operator
    // (destinationOperator) where did the column
    // actually get loaded
    public List<OutputColumn> traceLoadColumn(ObjectNode programNode,
                                              OutputColumn destColumn) throws LineageException
    {
        List<OutputColumn> loadColumns = new ArrayList<OutputColumn>();
        ColumnLineage columnNode = this.lineageInfo.getColumnLineageNode(destColumn);
        List<LineageGraphVertex> list1 =
                LineageGraph.traceTerminalNodes(columnNode, new String[] { "LOAD" }, false);
        List<LineageGraphVertex> list2 =
                LineageGraph.traceTerminalNodes(columnNode, new String[] { "LOAD-BLOCK" }, false);
        list1.addAll(list2);

        for (LineageGraphVertex graphNode : list1)
        {
            ColumnLineage colInfo = (ColumnLineage) graphNode;
            Pair<ObjectNode, JsonNode> phaseInfo =
                    this.getLineage().getPreLineage().getJobPhase(colInfo.node.opNode);
            if (LineageHelper.isLoadOperator(phaseInfo.getFirst(),
                                             phaseInfo.getSecond(),
                                             colInfo.node.opNode))
                loadColumns.add(colInfo.node);

        }
        return loadColumns;
    }

    

   

    public static String getOperatorType(ObjectNode jobNode,
                                         JsonNode phaseNode,
                                         ObjectNode opNode)
    {
        if (opNode.get("operator") != null)
            return opNode.get("operator").getTextValue();
        if (LineageHelper.isStoreCommand(jobNode, phaseNode, opNode))
            return "STORE";
        if (LineageHelper.isLoadOperator(jobNode, phaseNode, opNode))
            return "LOAD";

        // LineageHelper.trace("NULL operator type for " + opNode.toString() + "\n**********");
        // LineageHelper.trace("phaseNode = " + phaseNode.toString() + "\n*************");
        // LineageHelper.trace("jobNode = " + jobNode.toString() + "\n****************");
        throw new RuntimeException("Cannot find operatorType");
    }

    public static int getColumnIndexFromOutSchema(ObjectNode opNode,
                                                  String outputColumnName)
    {
        String[] outColNames = new BlockSchema(opNode.get("schema")).getColumnNames();
        int colid;
        for (colid = 0; colid < outColNames.length; colid++)
        {
            if (outColNames[colid].equals(outputColumnName))
                return colid;
        }
        return -1;
    }

    public static class LineageBuilder implements OperatorVisitor
    {
        public HashMap<Integer, OperatorLineage> opLineageMap =
                new HashMap<Integer, OperatorLineage>();
        public HashMap<OutputColumn, ColumnLineage> columnLineageMap =
                new HashMap<OutputColumn, ColumnLineage>();
        private LineageHelper preLineageInfo;
        public boolean exception = false;

        public LineageBuilder(LineageHelper preLineageInfo)
        {
            this.preLineageInfo = preLineageInfo;
        }

        public LineageHelper getPreLineage()
        {
            return this.preLineageInfo;

        }

        private <T> void operatorMapPut(HashMap<Integer, T> operatorMap,
                                        ObjectNode operatorNode,
                                        T valueObj)
        {
            Integer opSequence = this.preLineageInfo.getOpSequence(operatorNode);
            operatorMap.put(opSequence, valueObj);

        }

        private <T> T operatorMapGet(HashMap<Integer, T> operatorMap,
                                     ObjectNode operatorNode)
        {
            Integer opSequence = this.preLineageInfo.getOpSequence(operatorNode);
            return operatorMap.get(opSequence);
        }

        public boolean inspect(ObjectNode jobNode,
                               JsonNode phaseNode,
                               ObjectNode operatorNode)
        {
            ArrayList<ObjectNode> sourceOperators =
                    preLineageInfo.findAllOperatorSources(jobNode,
                                                          phaseNode,
                                                          operatorNode);

            if (sourceOperators != null)
            {
                for (ObjectNode sourceNode : sourceOperators)
                    addOperatorLineage(sourceNode, operatorNode);
            }

            Pair<ObjectNode, JsonNode> jobPhase =
                    preLineageInfo.getJobPhase(operatorNode);
            if (jobPhase.getSecond() != phaseNode)
                throw new RuntimeException("mis-matched phaseNode stored in phaseMap for \nopNode= "
                        + operatorNode.toString()
                        + "\nphaseNode = "
                        + jobPhase.getSecond().toString()
                        + "\noriginal phaseNode = "
                        + phaseNode.toString());

            LineageHelper.trace("Lineage Visitor visiting " + operatorNode.toString());
            // Now capture columnLineage for all output columns at this node
            BlockSchema outSchema = new BlockSchema(operatorNode.get("schema"));
            for (String colName : outSchema.getColumnNames())
            {
                OutputColumn destColumn = new OutputColumn(operatorNode, colName);

                List<OutputColumn> sourceColumns =
                        getSourceColumns(operatorNode, colName);
                if (sourceColumns != null)
                {
                    for (OutputColumn sourceColumn : sourceColumns)
                        addColumnLineage(sourceColumn, destColumn);
                }
            }
            return true;
        }

        private void addColumnLineage(OutputColumn sourceColumn, OutputColumn destColumn)
        {
            // TODO Auto-generated method stub
            ColumnLineage sourceLineage = getColumnLineageNode(sourceColumn);
            ColumnLineage destLineage = getColumnLineageNode(destColumn);

            if (CommonUtils.indexOfByRef(destLineage.parentNodes, sourceLineage) == -1)
                destLineage.parentNodes.add(sourceLineage);

            if (CommonUtils.indexOfByRef(sourceLineage.childNodes, destLineage) == -1)
            {
                sourceLineage.childNodes.add(destLineage);
                if (true)
                {
                    LineageHelper.trace("Adding columnLineage ");
                    LineageHelper.trace("****************\n Source = " + sourceLineage);
                    LineageHelper.trace("****************\n Dest =" + destLineage);
                }
            }
        }

        private void addOperatorLineage(ObjectNode sourceNode, ObjectNode operatorNode)
        {

            if (sourceNode == null)
                throw new RuntimeException("Null source node for operator "
                        + operatorNode);
            OperatorLineage sourceLineage = getOperatorLineageNode(sourceNode);
            OperatorLineage destLineage = getOperatorLineageNode(operatorNode);
            if (CommonUtils.indexOfByRef(destLineage.parentNodes, sourceLineage) == -1)
                destLineage.parentNodes.add(sourceLineage);

            if (CommonUtils.indexOfByRef(sourceLineage.childNodes, destLineage) == -1)
            {
                sourceLineage.childNodes.add(destLineage);
                if (true)
                {
                    LineageHelper.trace("adding operator lineage\n*********** \nsourceNode\n*********************\n"
                            + sourceNode.toString());
                    LineageHelper.trace("\ndestNode\n*************\n" + operatorNode.toString());
                }
            }

        }

        public OperatorLineage getOperatorLineageNode(ObjectNode sourceNode)
        {
            OperatorLineage result = this.operatorMapGet(opLineageMap, sourceNode);
            if (result != null)
                return result;
            result = new OperatorLineage(sourceNode, getNodeType(sourceNode));
            operatorMapPut(opLineageMap, sourceNode, result);
            return result;
        }

        private String getNodeType(ObjectNode sourceNode)
        {
            Pair<ObjectNode, JsonNode> jobPhase =
                    this.preLineageInfo.getJobPhase(sourceNode);

            if (jobPhase == null)
            {
                throw new RuntimeException("PhaseInformation missing for "
                        + sourceNode.toString() + "\n*********");
            }

            return getOperatorType(jobPhase.getFirst(), jobPhase.getSecond(), sourceNode);

        }

        public ColumnLineage getColumnLineageNode(OutputColumn sourceColumn)
        {
            ColumnLineage result = this.columnLineageMap.get(sourceColumn);
            if (result != null)
                return result;
            result = new ColumnLineage(sourceColumn, getNodeType(sourceColumn.opNode));
            this.columnLineageMap.put(sourceColumn, result);
            return result;

        }

        /* Main module that traces the lineage of a column (specified in outputColumnName) across an operator specified in opNode.
       */
        public List<OutputColumn> getSourceColumns(ObjectNode opNode,
                                                   String outputColumnName)
        {
            List<OutputColumn> sourceColumns = new ArrayList<OutputColumn>();

            Pair<ObjectNode, JsonNode> jobPhase = this.preLineageInfo.getJobPhase(opNode);
            String opType =
                    getOperatorType(jobPhase.getFirst(), jobPhase.getSecond(), opNode);

            if (opType.equalsIgnoreCase("LOAD") || opType.equalsIgnoreCase("LOAD_BLOCK"))
            {
                List<ObjectNode> storeNodes =
                        this.preLineageInfo.findAllParentStores(jobPhase.getFirst(),
                                                                jobPhase.getSecond(),
                                                                opNode);
                if (storeNodes == null || storeNodes.size() == 0)
                {
                    LineageHelper.trace("Cannot find matching parent STORE for " + opNode.toString());
                    return sourceColumns;
                }

                // LineageHelper.trace("Found matching parent store for opNode " +
                // opNode.toString() + " storeNodes = " +
                // CommonUtils.listAsString(storeNodes));
                for (ObjectNode storeNode : storeNodes)
                {
                    int colid = getColumnIndexFromOutSchema(opNode, outputColumnName);
                    if (colid == -1)
                        throw new RuntimeException("Cannot find column "
                                + outputColumnName + " in operator " + opNode.toString());
                    String inputColName =
                            new BlockSchema(storeNode.get("schema")).getColumnNames()[colid];
                    sourceColumns.add(new OutputColumn(storeNode, inputColName));
                }

            }
            else if (opType.equals("STORE"))
            {
                List<ObjectNode> sourceOps =
                        preLineageInfo.findOperatorInputSources(opNode,
                                                                opNode.get("name")
                                                                      .getTextValue());
                for (ObjectNode sourceOp : sourceOps)
                    sourceColumns.add(new OutputColumn(sourceOp, outputColumnName));
            }
            else if (opType.equalsIgnoreCase("GENERATE"))
            {
                List<ObjectNode> sourceOps =
                        preLineageInfo.findOperatorInputSources(opNode,
                                                                opNode.get("input")
                                                                      .getTextValue());
                for (ObjectNode sourceOp : sourceOps)
                {
                    for (JsonNode exprNode : (ArrayNode) (opNode.get("outputTuple")))
                    {
                        if (exprNode.get("col_name")
                                    .getTextValue()
                                    .equals(outputColumnName))
                        {
                            List<String> inputColumns =
                                    getExpressionColumns(sourceOp,
                                                         exprNode.get("expression"));
                            if (inputColumns == null)
                                continue;
                            for (String icol : inputColumns)
                                sourceColumns.add(new OutputColumn(sourceOp, icol));
                        }
                    }
                }
            }
            else if (opType.equalsIgnoreCase("HASHJOIN")
                    || opType.equalsIgnoreCase("JOIN"))
            {
                String[] splits = outputColumnName.split("___");
                List<ObjectNode> sourceOps =
                        preLineageInfo.findOperatorInputSources(opNode, splits[0]);

                for (ObjectNode sourceOp : sourceOps)
                    sourceColumns.add(new OutputColumn(sourceOp, splits[1]));
                // TODO : for join key add lineage to both parents.
                addJoinKeyLineageToOtherInput(opNode, sourceColumns, 
                                              splits[0], splits[1]);
            }
            else if (opType.equalsIgnoreCase("GROUP_BY")
                     || opType.equalsIgnoreCase("CUBE"))

            {
                // add lineage from each input measure to group by output
                List<ObjectNode> sourceOps =
                        preLineageInfo.findOperatorInputSources(opNode,
                                                                opNode.get("input")
                                                                      .getTextValue());
                for (ObjectNode sourceOp : sourceOps)
                {
                    ArrayNode aggregates = (ArrayNode) opNode.get("aggregates");
                    for (JsonNode aggNode : aggregates)
                    {
                        String[] inputCols = JsonUtils.asArray(aggNode, "input");
                        String dest = ((ObjectNode) aggNode).get("output").getTextValue();
                        if (dest.equals(outputColumnName)){
                          for (String inputCol: inputCols)
                            sourceColumns.add(new OutputColumn(sourceOp, inputCol));
                        }
                    }


                    // add lineage from gbyCols or dimensions
                    String[] gbyCols = JsonUtils.asArray(opType.equals("GROUP_BY") ? opNode.get("groupBy") : opNode.get("dimensions"));
                    if (opNode.get("innerDimensions") != null)
                      gbyCols = CommonUtils.concat(gbyCols, JsonUtils.asArray(opNode.get("innerDimensions")));
                    for (String gbyCol: gbyCols) 
                    {
                      if (gbyCol.equals(outputColumnName))
                        sourceColumns.add(new OutputColumn(sourceOp, gbyCol));
                    }
                    
                }
            }
            else if (opType.equals("FLATTEN")){
              // TODO.
              this.exception = true;
            }
            // handle exception cases
            else if (opType.equals("USER_DEFINED_TUPLE_OPERATOR") || opType.equals("USER_DEFINED_BLOCK_OPERATOR") || opType.equals("RANK"))
            {
                this.exception = true;
            }
            else if (opNode.get("input") != null)
            {
                String[] inputRelations = RewriteUtils.getInputRelations(opNode);

                for (String inputRelation : inputRelations)
                {
                    List<ObjectNode> sourceOps =
                            preLineageInfo.findOperatorInputSources(opNode, inputRelation);
                    for (ObjectNode sourceOp : sourceOps)
                        sourceColumns.add(new OutputColumn(sourceOp, outputColumnName));
                }
            }
            return sourceColumns;
        }

        private void addJoinKeyLineageToOtherInput(ObjectNode opNode, 
                                                   List<OutputColumn> sourceCols,
                                                   String relationName,
                                                   String colName)
        {
          // nothing to do for outer joins
          if (opNode.get("joinType") != null)
            return;
          String[] inputRelations = JsonUtils.asArray(opNode.get("input"));
          int relpos =  inputRelations[0].equals(relationName) ? LEFT: RIGHT;
          String[] joinKeys = getJoinKeys(opNode,relpos);
          List<String> joinKeysList = Arrays.asList(joinKeys);
          int colIdx = joinKeysList.indexOf(colName);
          if (colIdx == -1)
            return;
          String[] otherJoinKeys = getJoinKeys(opNode, (relpos == LEFT ? RIGHT: LEFT));
          String otherColName = otherJoinKeys[colIdx];
          String otherRelationName = inputRelations[(relpos==LEFT? 1: 0)];
          List<ObjectNode> sourceOps = preLineageInfo.findOperatorInputSources(opNode, otherRelationName);
          for (ObjectNode sourceOp: sourceOps)
            sourceCols.add(new OutputColumn(sourceOp, otherColName));
        }

        // Find top level column names
        private List<String> getExpressionColumns(ObjectNode sourceOp, JsonNode exprNode)
        {
            List<String> resultList = new ArrayList<String>();
            getExpressionColumns(sourceOp, exprNode, resultList);
            if (resultList.size() == 0 && false)
                LineageHelper.trace("Get expression columns returning empty set\n SourceOp = "
                        + sourceOp.toString());
            return resultList;
        }

        private void getExpressionColumns(ObjectNode sourceOp,
                                          JsonNode exprNode,
                                          List<String> resultList)
        {
            String topColumn = null;
            if ((topColumn = checkTopLevelColumn(sourceOp, exprNode)) != null)
            {
                resultList.add(topColumn);
                return;
            }

            ArrayNode argsNode = (ArrayNode) exprNode.get("arguments");
            if (argsNode == null)
                return;
            for (JsonNode argNode : argsNode)
                getExpressionColumns(sourceOp, argNode, resultList);
        }

        private static String checkTopLevelColumn(ObjectNode sourceOp,
                                                  JsonNode genExprNode)
        {
            String topColName = null;
            if (!(genExprNode instanceof ObjectNode))
                return null;

            ObjectNode opNode = (ObjectNode) genExprNode;
            if (opNode.get("function") == null
                    || !opNode.get("function").getTextValue().equals("INPUT_PROJECTION"))
                return null;
            ArrayNode argsNode = (ArrayNode) opNode.get("arguments");
            if (argsNode.size() != 1 
                    || ((topColName = findNamedColumn(argsNode)) == null)
                    && (topColName = findIndexedColumn(sourceOp, argsNode)) == null)
                return null;
            return topColName;

        }

        private static String findIndexedColumn(ObjectNode sourceOp, ArrayNode argsNode)
        {
            BlockSchema sourceSchema = new BlockSchema(sourceOp.get("schema"));
            for (JsonNode arg : argsNode)
            {
              if (!arg.isNumber())
                return null;
              String colName =
                sourceSchema.getColumnNames()[arg.getIntValue()];
                return colName;
            }
            return null;

        }

        private static String findNamedColumn(ArrayNode argsNode)
        {
            for (JsonNode arg : argsNode)
            {
              if (!arg.isTextual())
                continue;
              return arg.getTextValue();
            }
            return null;
        }

    }

    

    public void buildLineage(ObjectNode programNode) throws LineageException
    {
        LineageHelper preLineageInfo = new LineageHelper();
        visitOperators(programNode, null, preLineageInfo);

        LineageBuilder lineageInfo = new LineageBuilder(preLineageInfo);
        visitOperators(programNode, null, lineageInfo);
        if (lineageInfo.exception)
          throw new LineageException("Cannot trace lineage");

        this.lineageInfo = lineageInfo;
        this.programNode = programNode;
    }

   

    public static boolean isJoinOperator(ObjectNode operatorNode)
    {
        if (operatorNode.get("operator") != null
                && (operatorNode.get("operator").getTextValue().equalsIgnoreCase("JOIN") || operatorNode.get("operator")
                                                                                                        .getTextValue()
                                                                                                        .equalsIgnoreCase("HASHJOIN")))
            return true;
        return false;
    }

    public boolean isDistinctGroupBy(ObjectNode gbyNode)
    {
        if (!JsonUtils.getText(gbyNode, "operator").equals("GROUP_BY"))
            return false;

        if (gbyNode.get("aggregates") != null)
            return false;

        ObjectNode inpNode =
                this.lineageInfo.preLineageInfo.findOperatorSource(gbyNode,
                                                                   gbyNode.get("input")
                                                                          .getTextValue());
        String[] inColumns = getSchemaOutputColumns(inpNode);
        String[] outColumns = getSchemaOutputColumns(gbyNode);
        if (inColumns.length != outColumns.length)
            return false;
        for (int i = 0; i < inColumns.length; i++)
        {
            if (inColumns[i].equals(outColumns[i]))
                return false;
        }
        return true;

    }

    public List<ObjectNode> traceColumnJoins(ObjectNode programNode,
                                             OutputColumn inputColumn) throws LineageException
    {
        LineageGraphVertex startVertex =
                this.lineageInfo.getOperatorLineageNode(inputColumn.opNode);
        List<LineageGraphVertex> allJoinDescendants =
                LineageGraph.traceTerminalNodes(startVertex, new String[] { "JOIN", "HASHJOIN" }, true);
        List<ObjectNode> columnJoins = new ArrayList<ObjectNode>();
        LineageGraphVertex columnVertex =
                this.lineageInfo.getColumnLineageNode(inputColumn);
        List<OutputColumn> colDescendants =
                traceStraightLineColumnDescendants(inputColumn);
        for (LineageGraphVertex vertex : allJoinDescendants)
        {
            OperatorLineage opl = (OperatorLineage) vertex;
            boolean columnJoin = false;
            for (OutputColumn colDesc : colDescendants)
            {
                if (onlyJoinKey(opl, colDesc))
                    columnJoin = true;
            }
            if (columnJoin)
                columnJoins.add(opl.node);
        }
        return columnJoins;

    }

    private static String[] getJoinKeys(ObjectNode joinNode, int leftRight)
    {
        if (leftRight == LEFT)
            return (joinNode.get("operator").getTextValue().equals("JOIN")
                    ? JsonUtils.asArray(joinNode.get("leftCubeColumns"))
                    : JsonUtils.asArray(joinNode.get("leftJoinKeys")));
        else
            return (joinNode.get("operator").getTextValue().equals("JOIN")
                    ? JsonUtils.asArray(joinNode.get("rightCubeColumns"))
                    : JsonUtils.asArray(joinNode.get("rightJoinKeys")));
    }

    private boolean onlyJoinKey(OperatorLineage opl, OutputColumn colDesc)
    {
        List<LineageGraphVertex> opInputs = opl.getParentVertices();
        boolean columnIsInput = false;
        for (LineageGraphVertex opInput : opInputs)
        {
            if (((OperatorLineage) opInput).node == colDesc.opNode)
                columnIsInput = true;
        }
        if (!columnIsInput)
            return false;

        Pair<ObjectNode, JsonNode> jobPhase =
                this.lineageInfo.preLineageInfo.getJobPhase(colDesc.opNode);
        String inRelation =
                LineageHelper.getOperatorOutput(jobPhase.getFirst(),
                                                jobPhase.getSecond(),
                                                colDesc.opNode);
        String[] joinKeys = null;
        if (inRelation.equals(opl.node.get("leftBlock").getTextValue()))
            joinKeys = getJoinKeys(opl.node, LEFT);
        else
            joinKeys = getJoinKeys(opl.node, RIGHT);

        if (joinKeys.length == 1 && joinKeys[0].equals(colDesc.columnName))
            return true;
        return false;
    }

    public LineagePath tracePath(ObjectNode startOp, ObjectNode endOp)
    {
        return LineageGraph.tracePath(lineageInfo.getOperatorLineageNode(startOp),
                                      lineageInfo.getOperatorLineageNode(endOp));
    }

   private static class StraightLineColumnDescendantVisitor implements
                                                           LineageGraphVisitor
   {
     
     public List<OutputColumn> columnDescendants = new ArrayList<OutputColumn>();
     
     @Override
     public boolean visit(LineageGraphVertex graphNode)
       {
         ColumnLineage columnLineage = (ColumnLineage) graphNode;
         if (columnLineage.isExpressionOutput())
           return false;
        columnDescendants.add(columnLineage.node);
        return true;
       }
     
     @Override
     public void finishSubtree(LineageGraphVertex graphNode)
     {

     }
     
   }   
  
    private List<OutputColumn> traceStraightLineColumnDescendants(OutputColumn inputColumn)
    {
        StraightLineColumnDescendantVisitor visitorObj =
                new StraightLineColumnDescendantVisitor();
        LineageGraphVertex startVertex =
                this.lineageInfo.getColumnLineageNode(inputColumn);
        LineageGraph.visitLineageGraph(startVertex, visitorObj, true);
        return visitorObj.columnDescendants;
    }

    public List<ObjectNode> getOperatorSources(ObjectNode opNode)
    {
        Pair<ObjectNode, JsonNode> jobPhase =
                this.lineageInfo.preLineageInfo.getJobPhase(opNode);
        return this.lineageInfo.preLineageInfo.findAllOperatorSources(jobPhase.getFirst(),
                                                                      jobPhase.getSecond(),
                                                                      opNode);

    }

    public ArrayNode getPhaseOperators(JsonNode phaseNode)
    {
        return (ArrayNode) (LineageHelper.isReducePhase(phaseNode) ? (ArrayNode) phaseNode
                : ((ObjectNode) phaseNode).get("operators"));
    }

    public static void setPhaseOperators(ObjectNode jobNode,
                                         JsonNode phaseNode,
                                         ArrayNode phaseOps)
    {
        if (LineageHelper.isReducePhase(phaseNode))
            jobNode.put("reduce", phaseOps);
        else
            ((ObjectNode) phaseNode).put("operators", phaseOps);

    }

    public JsonNode getPhase(ObjectNode opNode)
    {
        return this.lineageInfo.preLineageInfo.getJobPhase(opNode).getSecond();
    }

    public Pair<ObjectNode, JsonNode> getJobPhase(ObjectNode opNode)
    {
        return this.lineageInfo.preLineageInfo.getJobPhase(opNode);
    }

    public static String traceIndexPath(ObjectNode jobNode, String indexName)
    {
        ArrayNode cacheIndexNode = (ArrayNode) (jobNode.get("cacheIndex"));
        for (JsonNode cacheIndex : cacheIndexNode)
        {
            if (((ObjectNode) cacheIndex).get("name").getTextValue().equals(indexName))
                return ((ObjectNode) cacheIndex).get("path").getTextValue();
        }
        return null;
    }

    public String[] getBlockgenPartitionKeys(ObjectNode bgNode)
    {
        ArrayNode pkeysNode =
                (isBlockgenByIndex(bgNode)
                        ? (ArrayNode) bgNode.get("originalPartitionKeys")
                        : (ArrayNode) bgNode.get("partitionKeys"));
        if (pkeysNode == null)
            return null;
        return JsonUtils.asArray(pkeysNode);
    }

    public String getBlockgenStorePath(ObjectNode programNode, ObjectNode bgNode)
    {
        List<ObjectNode> storeNodes = new ArrayList<ObjectNode>();
        List<ObjectNode> bgNodeDescents = traceOperatorDescendants(bgNode);
        Pair<ObjectNode, JsonNode> bgPhase =
                this.lineageInfo.preLineageInfo.getJobPhase(bgNode);
        for (ObjectNode descNode : bgNodeDescents)
        {
            if (getPhase(descNode) == bgPhase.getSecond()
                    && LineageHelper.isStoreCommand(bgPhase.getFirst(), bgPhase.getSecond(), descNode))
                storeNodes.add(descNode);
        }
        if (storeNodes.size() > 1 || storeNodes.size() == 0)
            return null;
        return storeNodes.get(0).get("path").getTextValue();
    }

    public ObjectNode getMatchingLoadInJob(ObjectNode jobNode, String pathName)
    {
        for (Integer opNodeSequence : this.lineageInfo.preLineageInfo.loadPathsMap.keySet())
        {
            ObjectNode opNode =
                    this.lineageInfo.preLineageInfo.operatorList.get(opNodeSequence);
            if (getJobPhase(opNode).getFirst() != jobNode)
                continue;
            List<String> pathNames =
                    this.lineageInfo.preLineageInfo.operatorMapGet(this.lineageInfo.preLineageInfo.loadPathsMap,
                                                                   opNode);
            if (pathNames.indexOf(pathName) != -1)
                return opNode;
        }
        return null;
    }

    public List<ObjectNode> traceOperatorDescendants(ObjectNode opNode)
    {
        LineageGraphVertex startVertex = this.getLineage().getOperatorLineageNode(opNode);
        List<LineageGraphVertex> opDescents = LineageGraph.traceAllReachable(startVertex, true);

        List<ObjectNode> result = new ArrayList<ObjectNode>();
        for (LineageGraphVertex gv : opDescents)
            result.add(((OperatorLineage) gv).node);
        return result;
    }

    public static boolean isCountDistinctAggregate(ObjectNode operatorNode)
    {
        if (operatorNode.get("operator") == null)
          return false;

        String type = operatorNode.get("operator").getTextValue();
        if (!type.equals("GROUP_BY") && !type.equals("CUBE"))
            return false;

        if (!operatorNode.has("aggregates"))
            return false;

        for (JsonNode aggregateJson : operatorNode.path("aggregates"))
        {
            // Create the aggregator object
            JsonNode typeNode = aggregateJson.get("type");

            // Group by case
            if (typeNode.isTextual()){
              AggregationType aggType =
                AggregationType.valueOf(JsonUtils.getText(aggregateJson, "type"));
              String measureColumn = JsonUtils.getText(aggregateJson, "input");
              if (aggType != AggregationType.COUNT_DISTINCT)
                return false;
            }
            else if (typeNode instanceof ArrayNode){
              String[] typeArray = JsonUtils.asArray(aggregateJson, "type");
              if (!typeArray[0].equals("SUM") || !typeArray[1].equals("COUNT_TO_ONE"))
                return false;
            }
        }

        return true;
    }

    public ObjectNode getOperatorJobNode(ObjectNode opNode)
    {
        Pair<ObjectNode, JsonNode> jobPhase =
                this.lineageInfo.preLineageInfo.getJobPhase(opNode);
        return jobPhase.getFirst();
    }

    public List<ObjectNode> computeJoinsInJob(ObjectNode programNode, ObjectNode jobNode)
    {
        List<ObjectNode> joinsList = new ArrayList<ObjectNode>();
        for (ObjectNode opNode : this.lineageInfo.preLineageInfo.operatorList)
        {
            if (getOperatorJobNode(opNode) != jobNode || !isJoinOperator(opNode))
                continue;
            joinsList.add(opNode);
        }
        return joinsList;
    }

    public boolean isParentOperator(ObjectNode node1, ObjectNode node2)
    {
        LineageGraphVertex v1 = this.lineageInfo.getOperatorLineageNode(node1);
        LineageGraphVertex v2 = this.lineageInfo.getOperatorLineageNode(node2);
        if (CommonUtils.indexOfByRef(v1.getChildVertices(), v2) != -1)
            return true;
        return false;
    }

    public boolean isDescendantOf(ObjectNode ancestorNode, ObjectNode descNode)
    {
        List<ObjectNode> descList = this.traceOperatorDescendants(ancestorNode);
        if (CommonUtils.indexOfByRef(descList, descNode) != -1)
            return true;
        return false;
    }

    public int getDescendantInputIndex(String[] inputRelations,
                                       ObjectNode opNode,
                                       ObjectNode factNode)
    {
        ObjectNode[] inputSources = new ObjectNode[inputRelations.length];
        List<ObjectNode> factDescents = traceOperatorDescendants(factNode);

        for (int i = 0; i < inputRelations.length; i++)
        {
            inputSources[i] =
                    this.getLineage()
                        .getPreLineage()
                        .findOperatorSource(opNode, inputRelations[i]);
            if (CommonUtils.indexOfByRef(factDescents, inputSources[i]) != -1)
                return i;
        }
        return -1;

    }

    public List<LineagePath> traceMatchingPaths(ObjectNode startNode,
                                              ArrayList<String> nodeTypes,
                                              ObjectNode terminalNode,
                                              boolean isForward)
    {
      LineageGraphVertex terminalOpLineage =
        (terminalNode != null
         ? this.lineageInfo.operatorMapGet(this.lineageInfo.opLineageMap,
                                           terminalNode) : null);
      LineageGraphVertex startOpLineage =
        this.lineageInfo.operatorMapGet(this.lineageInfo.opLineageMap, startNode);
      return LineageGraph.traceMatchingPaths(startOpLineage, nodeTypes, terminalOpLineage, isForward);
    }

}
