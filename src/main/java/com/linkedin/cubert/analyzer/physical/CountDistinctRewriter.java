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
import com.linkedin.cubert.operator.OperatorType;
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
import com.linkedin.cubert.analyzer.physical.AggregateRewriter.AggregateRewriteException;

public class CountDistinctRewriter extends AggregateRewriter
{

    protected int nbitmapshifts = 0;
    protected static String BITMAP_COLUMN_NAME = "bitmap";

    public CountDistinctRewriter()
    {
        super();
    }

    @Override
    public boolean isRewritable(ObjectNode operatorNode)
    {
        String operatorType = operatorNode.get("operator").getTextValue();
        if (operatorType.equalsIgnoreCase("CUBE")
                || operatorType.equalsIgnoreCase("GROUP_BY")
                && Lineage.isCountDistinctAggregate(operatorNode))
            return true;
        return false;
    }

    public void rewriteFactBlockgenPath(ObjectNode cubeNode,
                                        LineagePath opSequencePath,
                                        ObjectNode factNode,
                                        Pair<ObjectNode, ObjectNode> bgInfo) throws AggregateRewriteException
    {
        // if CREATE_BLOCK is not one of the ultimate or penultimate opNodes, report
        // rewrite exception
        ObjectNode bgNode = bgInfo.getSecond();
        ArrayNode phaseOps = lineage.getPhaseOperators(lineage.getPhase(bgNode));
        int len = phaseOps.size();
        if (phaseOps.get(len - 1) != bgNode)
            throw new AggregateRewriteException("Xformed Fact CREATE_BLOCK should be last operator before store");

        /*
         * boolean formattedDateColumn = false; boolean regenerateAfterJoin = false;
         * String dateColumnAfterJoin = null;
         */
        getFactTimeSpecNode(factNode, cubeNode);
        formattedDateColumn = false;
        dateColumnAlias = tNode.get("dateColumn").getTextValue();

        for (LineageGraphVertex opNodeVertex : opSequencePath.nodes)
        {
            ObjectNode opNode = ((OperatorLineage) opNodeVertex).node;

            // if incompatible operator, trigger exception
            if (incompatibleWithBlockgenPath(opNode))
              throw new AggregateRewriteException("Found incompatible operator along fact blockgen path " + opNode);

            // If generate inject date column
            if (opNode.get("operator") != null
                    && opNode.get("operator").getTextValue().equalsIgnoreCase("GENERATE"))
            {
                ArrayNode generateArgs = (ArrayNode) (opNode.get("outputTuple"));
                if (!formattedDateColumn)
                {
                    JsonNode dateColumnNode =
                            generateDateColumn(cubeNode, factNode, bgInfo);

                    generateArgs.add(dateColumnNode);
                    formattedDateColumn = true;
                }
                else
                {

                    generateArgs.add(regenerateDateColumn(dateColumnAlias));

                }
                dateColumnAlias = DATE_COLUMN_NAME;
            }

            // If join operator, stash the renamed date column
            if (lineage.isJoinOperator(opNode))
            {
                BlockSchema joinSchema = new BlockSchema(opNode.get("schema"));
                String[] colNames = joinSchema.getColumnNames();
                String newAlias = dateColumnAlias;

                String[] joinInputs = JsonUtils.asArray(opNode.get("input"));
                int factIndex =
                        lineage.getDescendantInputIndex(joinInputs, opNode, factNode);
                newAlias = joinInputs[factIndex] + "___" + dateColumnAlias;

                // TODO add an explict generate if the very next statement after JOIN is
                // not a GENERATE.
                if (addGenerateAfterJoin(opNode))
                    continue;
                dateColumnAlias = newAlias;

            }

            // if group by on all columns OR distinct that is either immediately before
            // or after the CREATE_BLOCK, remove
            if (opNode.get("operator") != null
                    && (opNode.get("operator")
                              .getTextValue()
                              .equalsIgnoreCase("DISTINCT") || lineage.isDistinctGroupBy(opNode))
                    && lineage.isParentOperator(opNode, bgInfo.getSecond()))
            {
                JsonNode phaseNode = lineage.getPhase(opNode);
                JsonUtils.deleteFromArrayNode(phaseOps, opNode);

                // remember new input to bgNode
                this.factBgInput = opNode.get("input").getTextValue();
                bgInfo.getSecond().put("input", factBgInput);
            }
        }
    }

    private boolean addGenerateAfterJoin(ObjectNode joinNode)
    {
        JsonNode phaseNode = lineage.getPhase(joinNode);
        ObjectNode jobNode = lineage.getJobPhase(joinNode).getFirst();
        ArrayNode joinPhaseOps = lineage.getPhaseOperators(phaseNode);

        List<JsonNode> opsList = JsonUtils.toArrayList(joinPhaseOps);
        int ji = CommonUtils.indexOfByRef(opsList, joinNode);

        if (ji == opsList.size()
                || JsonUtils.getText(opsList.get(ji + 1), "operator").equals("GENERATE"))
            return false;

        System.out.println("Adding Explicit generate after " + joinNode);
        // Have to add explicit generate.
        String[] joinSchemaCols = lineage.getSchemaOutputColumns(joinNode);
        Object[] argsArray = new Object[joinSchemaCols.length];
        int nargs = 0;
        for (String colName : joinSchemaCols)
        {

            if (colName.endsWith(dateColumnAlias))
                argsArray[nargs++] = dateColumnAlias = DATE_COLUMN_NAME;
            else
                argsArray[nargs++] = colName;

            argsArray[nargs++] = colName;
        }

        ObjectNode generateNode =
                RewriteUtils.createGenerateNode(JsonUtils.getText(joinNode, "output"),
                                                JsonUtils.getText(joinNode, "output"),
                                                argsArray);

        joinPhaseOps =
                JsonUtils.insertNodeListAfter(joinPhaseOps,
                                              joinNode,
                                              Arrays.asList(new ObjectNode[] { generateNode }));
        lineage.setPhaseOperators(jobNode, phaseNode, joinPhaseOps);
        return true;
    }

    private JsonNode regenerateDateColumn(String inColName)
    {
        return RewriteUtils.createProjectionExpressionNode(DATE_COLUMN_NAME, inColName);
    }

    private JsonNode generateDateColumn(ObjectNode cubeNode,
                                        ObjectNode factNode,
                                        Pair<ObjectNode, ObjectNode> bgInfo) throws AggregateRewriteException
    {
        return generateDateColumn(dateColumnAlias, tNode.get("timeFormat").getTextValue());

    }

    private JsonNode generateDateColumn(String dateColumn, String timeFormat) throws AggregateRewriteException
    {
        String fmt = timeFormat.split(":")[0];

        if (fmt.equalsIgnoreCase("yyyy-MM-dd"))
            return RewriteUtils.createProjection(dateColumn);
        else if (fmt.equalsIgnoreCase("epoch"))
        {
            String timeZone = timeFormat.split(":")[1];
            return RewriteUtils.createFunctionExpressionNode(DATE_COLUMN_NAME,
                                                             "com.linkedin.dwh.udf.date.EpochToDateFormat",
                                                             RewriteUtils.createProjection(dateColumn),
                                                             RewriteUtils.createStringConstant("yyyy-MM-dd"),
                                                             RewriteUtils.createStringConstant(timeZone));
        }
        throw new AggregateRewriteException("Unsupported date column format for "
                + dateColumn + " timeFormat = " + timeFormat);
    }

    /* transform the fact pre blockgen. Should move into count distinct rewriter */
    public void transformFactPreBlockgen(ObjectNode programNode,
                                         Pair<ObjectNode, ObjectNode> bgInfo) throws AggregateRewriteException
    {
        insertIncrementalMultipleDayGroupBy(programNode, bgInfo);
    }

    private void insertIncrementalMultipleDayGroupBy(ObjectNode programNode,
                                                     Pair<ObjectNode, ObjectNode> bgInfo) throws AggregateRewriteException
    {
        String[] factColumns = lineage.getSchemaOutputColumns(bgInfo.getSecond());
        String[] sortKeys;
        String[] groupByColumns;

        if (lineage.isBlockgenByIndex(bgInfo.getSecond()))
        {
            ObjectNode jobNode = lineage.getOperatorJobNode(bgInfo.getSecond());
            sortKeys =      JsonUtils.asArray(((ObjectNode) (jobNode.get("shuffle"))).get("pivotKeys"));
            groupByColumns =
                    (String[]) ArrayUtils.addAll(new String[] { "BLOCK_ID" }, factColumns);
        }
        else {
            sortKeys = JsonUtils.asArray(bgInfo.getSecond().get("pivotKeys"));
            groupByColumns = factColumns;
        }

        // check sort key condition
        if (!CommonUtils.isPrefix(sortKeys, groupByColumns))
          throw new AggregateRewriteException("Blockgen of union fact not sorted by fact collumns");

        ArrayNode aggsNode = JsonUtils.createArrayNode();
        ArrayNode udafArgsNode = JsonUtils.createArrayNode();

        String startDateHyphenated = DateTimeUtilities.getHyphenated(this.factStartDate);
        udafArgsNode.add(startDateHyphenated);
        aggsNode.add(RewriteUtils.createObjectNode("type",
                                                   "USER_DEFINED_AGGREGATION",
                                                   "udaf",
                                                   "com.linkedin.cubert.operator.aggregate.PresenceBitmapUDAF",
                                                   "constructorArgs",
                                                   udafArgsNode,
                                                   "input",
                                                   DATE_COLUMN_NAME,
                                                   "output",
                                                   BITMAP_COLUMN_NAME));
        String blockgenInputRelation =
                lineage.getOperatorSources(bgInfo.getSecond())
                       .get(0)
                       .get("output")
                       .getTextValue();
        ObjectNode groupByNode =
                RewriteUtils.createObjectNode("operator",
                                              "GROUP_BY",
                                              "input",
                                              blockgenInputRelation,
                                              "output",
                                              blockgenInputRelation,
                                              "groupBy",
                                              JsonUtils.createArrayNode(groupByColumns),
                                              "aggregates",
                                              aggsNode);

        ArrayNode phaseOperators =
                lineage.getPhaseOperators(lineage.getPhase(bgInfo.getSecond()));
        int blockGenIndex = 0;
        for (; blockGenIndex < phaseOperators.size(); blockGenIndex++)
        {
            if (phaseOperators.get(blockGenIndex) == bgInfo.getSecond())
                break;
        }
        if (blockGenIndex == phaseOperators.size())
            throw new RuntimeException("Cannot find CREATE_BLOCK operator in phase operator list");
        phaseOperators.insert(blockGenIndex, groupByNode);
        // phaseOperators.insert(blockGenIndex + 1, generateNode);

    }

    public ObjectNode transformMVPreCombine(String mvName)
    {
        ObjectNode rshiftNode = (ObjectNode) createRshiftNode(mvName, preCubeLoadColumns);
        return rshiftNode;
    }

    private JsonNode createRshiftNode(String relName, String[] factColumns)
    {
        Object[] generateArgs = new Object[(factColumns.length + 1) * 2];

        int i;
        for (i = 0; i < factColumns.length; i++)
        {
            generateArgs[2 * i] = factColumns[i];
            generateArgs[2 * i + 1] = RewriteUtils.createProjection(factColumns[i]);
        }

        generateArgs[2 * i] = BITMAP_COLUMN_NAME;
        JsonNode projNode = RewriteUtils.createProjection(BITMAP_COLUMN_NAME);
        ObjectNode rshiftNode = null;
        boolean bitmasking = false;

        if (mvRefreshDateOverride != -1 && mvRefreshDateOverride < mvRefreshDate)
        {
            /*
             * we mask all bit positions between (refreshdate over-ride , refreshdate]
             * mvRefreshDate corresponds to the most significant (right most) bit
             * position. mvRefreshDateOverride is the most significant position that must
             * be retained following the bit masking.
             */
            int dateDiff;

            if (mvRefreshDateOverride == 0)
                dateDiff = 31;
            else
            {
                DateTime endD = (DateTimeUtilities.getDateTime(mvRefreshDate));
                DateTime startD = (DateTimeUtilities.getDateTime(mvRefreshDateOverride));

                dateDiff = Days.daysBetween(endD, startD).getDays();
                dateDiff = Math.max(dateDiff, 31);

                int mask = 0xffffffff >> (dateDiff);

                if (dateDiff <= 31)
                {
                    rshiftNode =
                            RewriteUtils.createFunctionNode("BITAND",
                                                            projNode,
                                                            RewriteUtils.createIntegerConstant(mask));
                    bitmasking = true;
                }
            }
        }

        nbitmapshifts =
                Days.daysBetween(DateTimeUtilities.getDateTime(factStartDate),
                                 DateTimeUtilities.getDateTime(mvHorizonDate)).getDays();

        if (bitmasking)
            rshiftNode =
                    RewriteUtils.createFunctionNode("RSHIFT",
                                                    rshiftNode,
                                                    RewriteUtils.createIntegerConstant(nbitmapshifts));
        else
            rshiftNode =
                    RewriteUtils.createFunctionNode("RSHIFT",
                                                    projNode,
                                                    RewriteUtils.createIntegerConstant(nbitmapshifts));

        generateArgs[2 * i + 1] = rshiftNode;

        return RewriteUtils.createGenerateNode(relName, relName, generateArgs);
    }

    public ObjectNode postCombineGroupBy()
    {
        ArrayNode aggsNode = JsonUtils.createArrayNode();
        aggsNode.add(RewriteUtils.createObjectNode("type",
                                                   "BITWISE_OR",
                                                   "input",
                                                   BITMAP_COLUMN_NAME,
                                                   "output",
                                                   BITMAP_COLUMN_NAME));
        ObjectNode groupByNode =
                RewriteUtils.createObjectNode("operator",
                                              "GROUP_BY",
                                              "input",
                                              combinedRelation,
                                              "output",
                                              combinedRelation,
                                              "groupBy",
                                              JsonUtils.createArrayNode(preCubeLoadColumns),
                                              "aggregates",
                                              aggsNode);
        return groupByNode;
    }

    public ObjectNode postCombineFilter()
    {
        return RewriteUtils.createObjectNode("operator",
                                             "FILTER",
                                             "input",
                                             combinedRelation,
                                             "output",
                                             combinedRelation,
                                             "filter",
                                             createPresenceBitmapPredicate());
    }

    private ObjectNode createPresenceBitmapPredicate()
    {
        ObjectNode bitmapCol = RewriteUtils.createProjection("bitmap");
        ObjectNode zeroNode = RewriteUtils.createIntegerConstant(0);
        return RewriteUtils.createFunctionNode("NE", bitmapCol, zeroNode);

    }

    // in ject pre-Cube xforms to handle time-series.
    public ObjectNode preCubeTransform()
    {
        return null;
    }

    @Override
    public String[] getMeasureColumns(ObjectNode cubeOperatorNode)
    {
        String operatorType = cubeOperatorNode.get("operator").getTextValue();
        if (operatorType.equalsIgnoreCase("CUBE"))
            return getCountDistinctMeasuresForCube(cubeOperatorNode);
        else
            return getCountDistinctMeasuresForGroupBy(cubeOperatorNode);

    }

    private String[] getCountDistinctMeasuresForGroupBy(ObjectNode operatorNode)
    {
        ArrayList<String> measureCols = new ArrayList<String>();
        if (!operatorNode.get("operator").getTextValue().equalsIgnoreCase("GROUP_BY"))
            return null;

        if (!operatorNode.has("aggregates"))
            return null;

        for (JsonNode aggregateJson : operatorNode.path("aggregates"))
        {
            // Create the aggregator object
            AggregationType aggType =
                    AggregationType.valueOf(JsonUtils.getText(aggregateJson, "type"));
            if (!aggType.toString().equalsIgnoreCase("COUNT_DISTINCT"))
                continue;
            String[] measureColumns = JsonUtils.asArray(aggregateJson.get("input"));
            measureCols.addAll(Arrays.asList(measureColumns));
        }
        return measureCols.toArray(new String[measureCols.size()]);
    }

    private boolean cubeCountDistinctAggregate(JsonNode aggNode)
    {
        if (aggNode.get("type").isTextual()
                && JsonUtils.getText(aggNode, "type").equals("COUNT_DISTINCT"))
            return true;

        if (!(aggNode.get("type") instanceof ArrayNode))
            return false;

        ArrayNode aggs = (ArrayNode) (aggNode.get("type"));
        if (aggs.get(0).getTextValue().equalsIgnoreCase("SUM")
                && aggs.get(1).getTextValue().equalsIgnoreCase("COUNT_TO_ONE"))
            return true;
        return false;
    }

    private String[] getCountDistinctMeasuresForCube(ObjectNode cubeOperatorNode)
    {
        ArrayNode aggregates = (ArrayNode) cubeOperatorNode.get("aggregates");
        ArrayList<String> result = new ArrayList<String>();
        for (JsonNode node : aggregates)
        {
            if (!cubeCountDistinctAggregate(node))
                continue;
            result.addAll(Arrays.asList(JsonUtils.asArray(node.get("input"))));
        }
        return result.toArray(new String[aggregates.size()]);
    }

    private boolean incompatibleWithBlockgenPath(ObjectNode opNode){
      if (opNode.get("operator") == null)
        return false;
      String optype = opNode.get("operator").getTextValue();
      OperatorType type = OperatorType.valueOf(optype);
      if (type == OperatorType.CREATE_BLOCK)
        return false;
      if (!type.isTupleOperator() || type == OperatorType.GROUP_BY )
        return true;
      return false;
    }

          

}
