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

// Generated from CubertPhysical.g4 by ANTLR 4.1

package com.linkedin.cubert.plan.physical;

import com.linkedin.cubert.antlr4.CubertPhysicalBaseListener;
import com.linkedin.cubert.antlr4.CubertPhysicalLexer;
import com.linkedin.cubert.antlr4.CubertPhysicalListener;
import com.linkedin.cubert.antlr4.CubertPhysicalParser;
import com.linkedin.cubert.antlr4.CubertPhysicalParser.*;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.functions.builtin.FunctionFactory;
import com.linkedin.cubert.io.IndexCacheable;
import com.linkedin.cubert.io.NeedCachedFiles;
import com.linkedin.cubert.operator.BlockOperator;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.operator.aggregate.AggregationFunctions;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * This class provides an empty implementation of {@link CubertPhysicalListener}, which
 * can be extended to create a listener which only needs to handle a subset of the
 * available methods.
 */
public class PhysicalParser
{
    public static final class ErrorRecognizer extends BaseErrorListener
    {
        boolean hasErrors = false;

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line,
                                int charPositionInLine,
                                String msg,
                                RecognitionException e)
        {
            super.syntaxError(recognizer,
                              offendingSymbol,
                              line,
                              charPositionInLine,
                              msg,
                              e);
            hasErrors = true;
        }
    }

    public static void parseLocal(String fileName, String outFileName) throws FileNotFoundException,
            IOException,
            ParseException
    {

        CharStream inputStream = new ANTLRInputStream(new FileInputStream(fileName));

        FileOutputStream outStream = new FileOutputStream(outFileName);

        parseInputStream(inputStream, outStream);
    }

    public static ObjectNode parseProgram(String programString) throws IOException,
            ParseException
    {
        CharStream inputStream =
                new ANTLRInputStream(new ByteArrayInputStream(programString.getBytes()));
        return parseInputStream(inputStream);
    }

    private static ObjectNode parseInputStream(CharStream inputStream) throws ParseException
    {
        PhysicalListener listener = parsingTask(inputStream);
        return listener.programNode;
    }

    public static void parseHdfs(String fileName)
    {

    }

    private static void parseInputStream(CharStream input, FileOutputStream outStream) throws ParseException

    {
        PhysicalListener listener = parsingTask(input);
        writeOutput(listener, outStream);
    }

    private static PhysicalListener parsingTask(CharStream input) throws ParseException
    {
        CubertPhysicalLexer lexer = new CubertPhysicalLexer(input);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        CubertPhysicalParser parser = new CubertPhysicalParser(tokenStream);

        ErrorRecognizer errorListener = new ErrorRecognizer();
        parser.addErrorListener(errorListener);

        ParseTree ptree = parser.program();

        ParseTreeWalker walker = new ParseTreeWalker();
        PhysicalListener listener = new PhysicalListener();
        listener.input = input;

        walker.walk(listener, ptree);

        if (errorListener.hasErrors)
        {
            System.err.println("\nCannot parse cubert script. Exiting.");
            throw new ParseException(null, 0);
        }

        return listener;
    }

    private static void writeOutput(PhysicalListener listener, FileOutputStream outStream)
    {
        try
        {

            if (true)
            {

                // pretty printing the json object
                @SuppressWarnings("deprecation")
                String prettyStr =
                        listener.objMapper.defaultPrettyPrintingWriter()
                                          .writeValueAsString(listener.programNode);

                outStream.write(prettyStr.getBytes());
            }

            outStream.flush();
        }
        catch (IOException e)
        {
            System.out.println("Error writing JSON output");
            e.printStackTrace();
        }
    }

    public static class PhysicalListener extends CubertPhysicalBaseListener
    {
        // private StringBuffer outBuffer = new StringBuffer();
        CharStream input;
        private String operatorCommandLhs = null;
        private long indexSequence = 0;
        private final String INDEX_PREFIX = "INDEX";
        private ArrayNode cacheIndexNode = null;
        // private StringBuffer cacheIndexBuffer = new StringBuffer();
        // private final HashMap<String, String> pathToIndexMap =
        // new HashMap<String, String>();
        public final ObjectMapper objMapper = new ObjectMapper();
        private ObjectNode programNode;
        private ObjectNode hadoopConfNode;
        private ArrayNode libjarsNode;

        private ObjectNode mapCommandsNode;

        private ObjectNode mapReduceJobNode;
        private ArrayNode reduceCommandsNode;
        private ObjectNode shuffleCommandNode;
        private boolean overwrite = false;

        private ArrayList<ObjectNode> operatorCommandsList;

        private ObjectNode operatorNode;

        private ArrayNode jobsNode;

        private ObjectNode outputCommandNode;

        private final Set<String> cachedFiles = new HashSet<String>();

        private final Map<String, ObjectNode> inlineDictionaries =
                new HashMap<String, ObjectNode>();

        private final HashMap<String, List<Object>> functionCtorArgs =
                new HashMap<String, List<Object>>();
        private final HashMap<String, String> functionAliasMap =
                new HashMap<String, String>();

        private boolean insideMultipassGroup = false;
        private int multipassIndex = 0;

        @Override
        public void enterProgramName(@NotNull ProgramNameContext ctx)
        {
        }

        @Override
        public void enterProgram(@NotNull ProgramContext ctx)
        {
            this.programNode = objMapper.createObjectNode();
            this.hadoopConfNode = objMapper.createObjectNode();
            this.libjarsNode = objMapper.createArrayNode();
        }

        @Override
        public void exitProgramName(@NotNull ProgramNameContext ctx)
        {
            this.programNode.put("program",
                                 CommonUtils.stripQuotes(ctx.STRING().getText()));
        }

        public void exitProgram(@NotNull ProgramContext ctx)
        {
            this.programNode.put("jobs", jobsNode);

        }

        @Override
        public void exitSetCommand(@NotNull SetCommandContext ctx)
        {
            String propName = ctx.uri().getText();

            JsonNode constantJson =
                    createConstantExpressionNode(ctx.constantExpression());

            String propValue =
                    CommonUtils.stripQuotes(constantJson.get("arguments")
                                                        .get(0)
                                                        .toString());

            if (propName.equalsIgnoreCase("overwrite"))
                overwrite = Boolean.parseBoolean(propValue);

            if (mapReduceJobNode == null)
            {
                hadoopConfNode.put(propName, propValue);
            }
            else
            {
                ObjectNode confNode = (ObjectNode) mapReduceJobNode.get("hadoopConf");
                if (confNode == null)
                {
                    confNode = objMapper.createObjectNode();
                    mapReduceJobNode.put("hadoopConf", confNode);
                }
                confNode.put(propName, propValue);
            }

        }

        @Override
        public void enterRegisterCommand(@NotNull RegisterCommandContext ctx)
        {
        }

        @Override
        public void exitRegisterCommand(@NotNull RegisterCommandContext ctx)
        {

            this.libjarsNode.add(cleanPath(ctx.path()));
        }

        @Override
        public void enterHeaderSection(@NotNull HeaderSectionContext ctx)
        {

        }

        @Override
        public void exitHeaderSection(@NotNull HeaderSectionContext ctx)
        {
            this.programNode.put("hadoopConf", hadoopConfNode);
            this.programNode.put("libjars", libjarsNode);
            this.jobsNode = objMapper.createArrayNode();

        }

        @Override
        public void enterInputCommand(@NotNull InputCommandContext ctx)
        {
            // this.inputCommandNode = objMapper.createObjectNode();
        }

        @Override
        public void exitInputCommand(@NotNull InputCommandContext ctx)
        {
            ObjectNode inputNode = objMapper.createObjectNode();
            addLine(ctx, inputNode);

            inputNode.put("name", ctx.ID().get(0).getText());

            if (ctx.format != null)
                inputNode.put("type", ctx.format.getText().toUpperCase());
            else
                inputNode.put("type", ctx.classname.getText());

            ObjectNode paramsNode = objMapper.createObjectNode();
            if (ctx.params() != null)
            {

                for (int i = 0; i < ctx.params().keyval().size(); i++)
                {
                    List<TerminalNode> kv = ctx.params().keyval(i).STRING();
                    paramsNode.put(CommonUtils.stripQuotes(kv.get(0).getText()),
                                   CommonUtils.stripQuotes(kv.get(1).getText()));
                }
            }
            inputNode.put("params", paramsNode);

            ArrayNode inputPathArray = createInputPathsNode(ctx.inputPaths());
            inputNode.put("path", inputPathArray);

            this.mapCommandsNode.put("input", inputNode);

        }

        private ArrayNode createInputPathsNode(InputPathsContext inputPathsContext)
        {
            ArrayNode inputPathArray = objMapper.createArrayNode();
            for (InputPathContext pctx : inputPathsContext.inputPath())
            {

                if (pctx.INT().size() != 0)
                {
                    ObjectNode pathNode = objMapper.createObjectNode();
                    pathNode.put("root", cleanPath(pctx.path()));
                    pathNode.put("startDate", pctx.INT().get(0).getText());
                    pathNode.put("endDate", pctx.INT().get(1).getText());
                    inputPathArray.add(pathNode);
                }
                else
                    inputPathArray.add(cleanPath(pctx.path()));

            }
            return inputPathArray;
        }

        @Override
        public void enterOutputCommand(@NotNull OutputCommandContext ctx)
        {
            this.outputCommandNode = objMapper.createObjectNode();
        }

        @Override
        public void exitOutputCommand(@NotNull OutputCommandContext ctx)
        {
            outputCommandNode.put("name", ctx.ID().get(0).getText());
            outputCommandNode.put("path", cleanPath(ctx.path()));
            if (ctx.format != null)
                outputCommandNode.put("type", ctx.format.getText().toUpperCase());
            else
                outputCommandNode.put("type", ctx.classname.getText());

            addLine(ctx, outputCommandNode);
            ObjectNode paramsNode = objMapper.createObjectNode();
            if (ctx.params() != null)
            {
                for (int i = 0; i < ctx.params().keyval().size(); i++)
                {
                    List<TerminalNode> kv = ctx.params().keyval(i).STRING();
                    paramsNode.put(CommonUtils.stripQuotes(kv.get(0).getText()),
                                   CommonUtils.stripQuotes(kv.get(1).getText()));
                }
            }
            outputCommandNode.put("params", paramsNode);

            if (!paramsNode.has("overwrite"))
                paramsNode.put("overwrite", Boolean.toString(overwrite));

        }

        @Override
        public void exitEncodeOperator(@NotNull EncodeOperatorContext ctx)
        {
            operatorNode.put("operator", "DICT_ENCODE");
            operatorNode.put("input", ctx.ID(0).getText());
            operatorNode.put("output", operatorCommandLhs);

            ArrayNode anode = objMapper.createArrayNode();

            for (TerminalNode id : ctx.columns().ID())
                anode.add(id.getText());

            operatorNode.put("columns", anode);

            if (ctx.path() == null)
            {
                ObjectNode dict = inlineDictionaries.get(ctx.dictname.getText());
                if (dict == null)
                    throw new RuntimeException("Dictionary " + ctx.dictname.getText()
                            + " is not available");
                operatorNode.put("dictionary", dict);
            }
            else
            {
                String dictionaryPath = cleanPath(ctx.path());
                operatorNode.put("path", dictionaryPath);
                cachedFiles.add(dictionaryPath);
            }

            if (ctx.nullas != null)
            {
                operatorNode.put("replaceNull", ctx.nullas.getText());
            }

            if (ctx.n != null)
            {
                operatorNode.put("replaceUnknownCodes", ctx.n.getText());
            }
        }

        @Override
        public void exitOperatorCommandLhs(@NotNull OperatorCommandLhsContext ctx)
        {

            this.operatorCommandLhs = new String(ctx.getText());
        }

        @Override
        public void exitDecodeOperator(@NotNull DecodeOperatorContext ctx)
        {
            operatorNode.put("operator", "DICT_DECODE");
            operatorNode.put("input", ctx.ID(0).getText());
            operatorNode.put("output", operatorCommandLhs);

            ArrayNode anode = objMapper.createArrayNode();

            for (TerminalNode id : ctx.columns().ID())
                anode.add(id.getText());

            operatorNode.put("columns", anode);

            if (ctx.path() == null)
            {
                ObjectNode dict = inlineDictionaries.get(ctx.dictname.getText());
                if (dict == null)
                    throw new RuntimeException("Dictionary " + ctx.dictname.getText()
                            + " is not available");
                operatorNode.put("dictionary", dict);
            }
            else
            {
                String dictionaryPath = cleanPath(ctx.path());
                operatorNode.put("path", dictionaryPath);
                cachedFiles.add(dictionaryPath);
            }

            if (ctx.unknownas != null )
            {
                operatorNode.put("replaceUnknownCodes", CommonUtils.stripQuotes(ctx.unknownas.getText()));
            }
        }

        @Override
        public void exitLoadBlockOperator(@NotNull LoadBlockOperatorContext ctx)
        {
            String indexName = generateIndexName();
            String path = cleanPath(ctx.path());

            ObjectNode cacheIndex = objMapper.createObjectNode();
            cacheIndex.put("name", indexName);
            cacheIndex.put("path", path);
            cacheIndexNode.add(cacheIndex);

            operatorNode.put("operator", "LOAD_BLOCK");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("index", indexName);
            operatorNode.put("inMemory", ctx.inmemory != null);
            operatorNode.put("path", path); // added for redundancy
        }

        @Override
        public void exitJoinOperator(@NotNull JoinOperatorContext ctx)
        {
            operatorNode.put("operator", "JOIN");
            ArrayNode inputListNode = objMapper.createArrayNode();
            inputListNode.add(ctx.ID().get(0).getText());
            inputListNode.add(ctx.ID().get(1).getText());

            if (ctx.joinType() != null)
                operatorNode.put("joinType", ctx.joinType().getText());
            operatorNode.put("input", inputListNode);
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("leftBlock", ctx.ID().get(0).getText());
            operatorNode.put("leftCubeColumns", createIDListNode(ctx.columns()
                                                                    .get(0)
                                                                    .ID()));

            operatorNode.put("rightCubeColumns", createIDListNode(ctx.columns()
                                                                     .get(1)
                                                                     .ID()));

            if (ctx.joinType() != null)
                operatorNode.put("joinType", ctx.joinType().getText().toUpperCase());

        }

        @Override
        public void exitHashJoinOperator(@NotNull HashJoinOperatorContext ctx)
        {
            operatorNode.put("operator", "HASHJOIN");
            ArrayNode inputListNode = objMapper.createArrayNode();
            inputListNode.add(ctx.ID().get(0).getText());
            inputListNode.add(ctx.ID().get(1).getText());
            operatorNode.put("input", inputListNode);
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("leftBlock", ctx.ID().get(0).getText());
            operatorNode.put("leftJoinKeys", createIDListNode(ctx.columns().get(0).ID()));
            operatorNode.put("rightJoinKeys", createIDListNode(ctx.columns().get(1).ID()));
            if (ctx.joinType() != null)
            {
                operatorNode.put("joinType", ctx.joinType().getText());
            }
        }

        @Override
        public void enterMapCommands(@NotNull MapCommandsContext ctx)
        {
            this.mapCommandsNode = objMapper.createObjectNode();
            this.operatorCommandsList = new ArrayList<ObjectNode>();
        }

        @Override
        public void exitMapCommands(@NotNull MapCommandsContext ctx)
        {
            ArrayNode operators = objMapper.createArrayNode();
            for (ObjectNode opNode : operatorCommandsList)
                operators.add(opNode);

            mapCommandsNode.put("operators", operators);

            ((ArrayNode) this.mapReduceJobNode.get("map")).add(mapCommandsNode);
        }

        @Override
        public void enterReduceCommands(@NotNull ReduceCommandsContext ctx)
        {
            this.reduceCommandsNode = objMapper.createArrayNode();
            this.operatorCommandsList = new ArrayList<ObjectNode>();

        }

        @Override
        public void exitReduceCommands(@NotNull ReduceCommandsContext ctx)
        {
            for (ObjectNode opNode : this.operatorCommandsList)
                this.reduceCommandsNode.add(opNode);
        }

        @Override
        public void exitShuffleCommand(@NotNull ShuffleCommandContext ctx)
        {
            this.shuffleCommandNode = objMapper.createObjectNode();

            addLine(ctx, shuffleCommandNode);
            shuffleCommandNode.put("name", ctx.ID().getText());
            shuffleCommandNode.put("type", "SHUFFLE");
            shuffleCommandNode.put("partitionKeys", createIDListNode(ctx.columns()
                                                                        .get(0)
                                                                        .ID()));
            if (ctx.columns().size() > 1)
                shuffleCommandNode.put("pivotKeys", createIDListNode(ctx.columns()
                                                                        .get(1)
                                                                        .ID()));
            else
                shuffleCommandNode.put("pivotKeys", createIDListNode(ctx.columns()
                                                                        .get(0)
                                                                        .ID()));

            if (ctx.aggregateList() != null)
                emitAggregateFunctions(ctx.aggregateList(), shuffleCommandNode);

        }

        @Override
        public void enterOperatorCommand(@NotNull OperatorCommandContext ctx)
        {
            this.operatorNode = objMapper.createObjectNode();
        }

        @Override
        public void exitOperatorCommand(@NotNull OperatorCommandContext ctx)
        {
            if (this.operatorNode.size() > 0)
            {
                addLine(ctx, operatorNode);
                this.operatorCommandsList.add(this.operatorNode);

                if (insideMultipassGroup)
                {
                    this.operatorNode.put("multipassIndex", multipassIndex);
                }
            }
        }

        @Override
        public void enterMultipassGroup(@NotNull MultipassGroupContext ctx)
        {
            insideMultipassGroup = true;
        }

        @Override
        public void exitMultipassGroup(@NotNull MultipassGroupContext ctx)
        {
            insideMultipassGroup = false;
        }

        @Override
        public void exitSinglePassGroup(@NotNull SinglePassGroupContext ctx)
        {
            multipassIndex++;
        }

        @Override
        public void exitGroupByOperator(@NotNull GroupByOperatorContext ctx)
        {
            operatorNode.put("operator", "GROUP_BY");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
            if (ctx.ALL() == null)
            {
                operatorNode.put("groupBy", createIDListNode(ctx.columns().ID()));
            }
            else
            {
                operatorNode.put("groupBy", objMapper.createArrayNode());
            }

            if (ctx.aggregateList() != null)
                emitAggregateFunctions(ctx.aggregateList(), operatorNode);
            if (ctx.summaryRewriteClause() != null)
                setupSummaryRewrite(operatorNode, ctx.summaryRewriteClause());

        }

        private void setupSummaryRewrite(ObjectNode cubeNode,
                                         SummaryRewriteClauseContext summaryRewriteClause)
        {
            cubeNode.put("summaryRewrite", "true");
            cubeNode.put("mvName", summaryRewriteClause.ID().getText());
            cubeNode.put("mvPath", cleanPath(summaryRewriteClause.path()));
            ArrayNode timeColumnSpecNode = objMapper.createArrayNode();
            for (TimeColumnSpecContext specCtx : summaryRewriteClause.timeColumnSpec())
            {
                // timeFormat can be "DAY or EPOCH:<timeZone>"
                String timeFormat = CommonUtils.stripQuotes(specCtx.timeFormat.getText());

                JsonNode specNode =
                        JsonUtils.createObjectNode("factPath",
                                                   CommonUtils.stripQuotes(specCtx.factPath.STRING()
                                                                                           .getText()),
                                                   "dateColumn",
                                                   (specCtx.dateColumn.getText()),
                                                   "timeFormat",
                                                   timeFormat);
                timeColumnSpecNode.add(specNode);
            }
            cubeNode.put("timeColumnSpec", timeColumnSpecNode);
        }

        private void emitAggregateFunctions(AggregateListContext aggregateList,
                                            ObjectNode parent)
        {
            ArrayNode aggregatesNode = objMapper.createArrayNode();
            parent.put("aggregates", aggregatesNode);

            for (AggregateContext aggContext : aggregateList.aggregate())
            {
                ObjectNode aggNode = objMapper.createObjectNode();
                String aggFunction = aggContext.aggregationFunction().getText();
                aggNode.put("type", aggContext.aggregationFunction().getText());

                ArrayNode anode = objMapper.createArrayNode();
                if (aggContext.parameters != null)
                    for (TerminalNode id : aggContext.parameters.ID())
                        anode.add(id.getText());

                aggNode.put("input", anode);

                if (aggContext.ID() != null)
                    aggNode.put("output", aggContext.ID().getText());

                aggregatesNode.add(aggNode);
                if (!AggregationFunctions.isUserDefinedAggregation(aggFunction))
                    continue;

                aggNode.put("type", "USER_DEFINED_AGGREGATION");
                aggNode.put("udaf", aggFunction);
                List<Object> constructorArgs = this.functionCtorArgs.get(aggFunction);
                ArrayNode constructorArgsNode =
                        createConstructorArgsNode(aggFunction, constructorArgs);
                aggNode.put("constructorArgs", constructorArgsNode);
            }
        }

        @Override
        public void exitGenerateOperator(@NotNull GenerateOperatorContext ctx)
        {
            operatorNode.put("operator", "GENERATE");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("outputTuple",
                             createGenerateExpressionListNode(ctx.generateExpressionList()));

        }

        @Override
        public void exitFilterOperator(@NotNull FilterOperatorContext ctx)
        {
            operatorNode.put("operator", "FILTER");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("filter", createExpressionNode(ctx.expression()));

        }

        @Override
        public void exitFlattenOperator(FlattenOperatorContext ctx)
        {
            operatorNode.put("operator", "FLATTEN");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("genExpressions", generateFlattenNode(ctx));
        }

        private ArrayNode generateFlattenNode(FlattenOperatorContext ctx)
        {
            ArrayNode result = objMapper.createArrayNode();
            ;

            List<FlattenItemContext> flattenItems = ctx.flattenItem();
            for (FlattenItemContext fic : flattenItems)
            {
                ObjectNode itemNode = objMapper.createObjectNode();
                String col = fic.ID().getText();
                itemNode.put("col", col);
                FlattenTypeContext ft = fic.flattenType();

                if (ft != null)
                {
                    String flattenType = ft.getText();
                    if (flattenItems == null || flattenItems.equals(""))
                    {
                        continue;
                    }

                    itemNode.put("flatten", flattenType);
                }

                // generate out put schema information
                ArrayNode outCols = objMapper.createArrayNode();
                itemNode.put("output", outCols);

                TypeDefinitionsContext tds = fic.typeDefinitions();
                List<TypeDefinitionContext> typeDefList = tds.typeDefinition();

                for (TypeDefinitionContext tdc : typeDefList)
                {
                    ObjectNode colNode = objMapper.createObjectNode();

                    colNode.put("col", tdc.ID(0).getText());
                    colNode.put("type", tdc.ID(1).getText());

                    outCols.add(colNode);
                }

                result.add(itemNode);

            }
            return result;
        }

        @Override
        public void exitLimitOperator(@NotNull LimitOperatorContext ctx)
        {
            operatorNode.put("operator", "LIMIT");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("maxTuples", Integer.parseInt(ctx.INT().getText()));
        }

        @Override
        public void exitDistinctOperator(@NotNull DistinctOperatorContext ctx)
        {
            operatorNode.put("operator", "DISTINCT");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);
        }

        @Override
        public void enterMapReduceJob(@NotNull MapReduceJobContext ctx)
        {
            this.mapReduceJobNode = objMapper.createObjectNode();
            this.mapReduceJobNode.put("pigudfs", objMapper.createObjectNode());
            this.cachedFiles.clear();
            this.mapReduceJobNode.put("map", objMapper.createArrayNode());

            this.shuffleCommandNode = null;
            this.reduceCommandsNode = null;
            this.cacheIndexNode = objMapper.createArrayNode();
        }

        @Override
        public void exitMapReduceJob(@NotNull MapReduceJobContext ctx)
        {
            mapReduceJobNode.put("name", CommonUtils.stripQuotes(ctx.STRING().getText()));

            int mappersCount =
                    (ctx.mappersCount == null) ? 0
                            : Integer.parseInt(ctx.mappersCount.getText());
            int reducersCount =
                    (ctx.reducersCount == null) ? 0
                            : Integer.parseInt(ctx.reducersCount.getText());

            mapReduceJobNode.put("mappers", mappersCount);
            mapReduceJobNode.put("reducers", reducersCount);

            // removing and adding the "map" field in the json object
            // stupid hack to ensure that the map section is printed after name and
            // reducers (only needed for pretty printing)
            JsonNode mapNode = mapReduceJobNode.get("map");
            mapReduceJobNode.remove("map");
            mapReduceJobNode.put("map", mapNode);

            mapReduceJobNode.put("shuffle", this.shuffleCommandNode);
            mapReduceJobNode.put("reduce", this.reduceCommandsNode);
            if (this.cacheIndexNode != null)
                mapReduceJobNode.put("cacheIndex", this.cacheIndexNode);
            if (!this.cachedFiles.isEmpty())
            {
                ArrayNode anode = objMapper.createArrayNode();
                for (String cachedFile : cachedFiles)
                    anode.add(cachedFile);
                mapReduceJobNode.put("cachedFiles", anode);
            }
            mapReduceJobNode.put("output", this.outputCommandNode);
            this.cacheIndexNode = objMapper.createArrayNode();
            jobsNode.add(mapReduceJobNode);
        }

        @Override
        public void exitDuplicateOperator(@NotNull DuplicateOperatorContext ctx)
        {
            operatorNode.put("operator", "DUPLICATE");
            operatorNode.put("input", ctx.ID(0).getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("times", Integer.parseInt(ctx.INT().getText()));
            if (ctx.ID().size() > 1)
            {
                operatorNode.put("counter", ctx.ID(1).getText());
            }
        }

        @Override
        public void exitValidateOperator(@NotNull ValidateOperatorContext ctx)
        {
            super.exitValidateOperator(ctx);
            operatorNode.put("operator", "VALIDATE");
            operatorNode.put("input", ctx.ID(0).getText());
            operatorNode.put("output", this.operatorCommandLhs);
            operatorNode.put("blockgenType", "BY_" + ctx.ID().get(1).getText().toUpperCase());

            if (ctx.blockgenValue != null)
            {
                operatorNode.put("blockgenValue",
                                 Long.parseLong(ctx.blockgenValue.getText()));
            }
            if (ctx.path() != null)
            {
                String path = cleanPath(ctx.path());
                String indexName = generateIndexName();
                this.cacheIndexNode.add(JsonUtils.createObjectNode("name",
                                                                   indexName,
                                                                   "path",
                                                                   path));
                operatorNode.put("index", indexName);
            }
            operatorNode.put("partitionKeys", createIDListNode(ctx.columns().get(0).ID()));
            if (ctx.columns().size() == 2)
            {
                operatorNode.put("pivotKeys", createIDListNode(ctx.columns().get(1).ID()));

            }
        }

        @Override
        public void exitNoopOperator(NoopOperatorContext ctx)
        {
            operatorNode.put("operator", "NO_OP");
            operatorNode.put("input", ctx.ID().getText());
            operatorNode.put("output", this.operatorCommandLhs);

            if (ctx.partitionKeys != null)
                operatorNode.put("assertPartitionKeys",
                                 createIDListNode(ctx.partitionKeys.ID()));

            if (ctx.sortKeys != null)
                operatorNode.put("assertSortKeys", createIDListNode(ctx.sortKeys.ID()));
        }

        // @Override
        // public void exitDictionaryJob(@NotNull
        // CubertPhysicalParser.DictionaryJobContext ctx)
        // {
        // ObjectNode dictionaryJobNode = objMapper.createObjectNode();
        // dictionaryJobNode.put("name", CommonUtils.stripQuotes(ctx.STRING().getText()));
        // dictionaryJobNode.put("reducers", 1);
        // dictionaryJobNode.put("jobType", "GENERATE_DICTIONARY");
        //
        // ArrayNode mapsNode = objMapper.createArrayNode();
        // dictionaryJobNode.put("map", mapsNode);
        //
        // ObjectNode mapNode = objMapper.createObjectNode();
        // mapsNode.add(mapNode);
        //
        // ObjectNode inputNode = objMapper.createObjectNode();
        // inputNode.put("name", "inputRelation");
        // ArrayNode inputPathsNode = this.createInputPathsNode(ctx.inputPaths());
        // inputNode.put("path", inputPathsNode);
        // inputNode.put("type", "AVRO");
        // if (ctx.nullval != null)
        // inputNode.put("replaceNull", ctx.nullval.getText());
        //
        // if (ctx.defaultval != null)
        // inputNode.put("defaultValue", ctx.defaultval.getText());
        //
        // if (ctx.unsplittable != null)
        // inputNode.put("unsplittable", true);
        // else
        // inputNode.put("unsplittable", false);
        //
        // mapNode.put("input", inputNode);
        // mapNode.put("operators", objMapper.createArrayNode());
        //
        // ObjectNode outputNode = objMapper.createObjectNode();
        // outputNode.put("name", "inputRelation");
        // outputNode.put("path", cleanPath(ctx.path()));
        // outputNode.put("type", "AVRO");
        //
        // StringBuffer columnBuffer = new StringBuffer();
        // boolean first = true;
        // for (TerminalNode cctx : ctx.columns().ID())
        // {
        // columnBuffer.append((first ? "int " : ", int ") + cctx.getText());
        // first = false;
        // }
        // outputNode.put("columns", columnBuffer.toString());
        // dictionaryJobNode.put("output", outputNode);
        //
        // addLine(ctx, dictionaryJobNode);
        //
        // jobsNode.add(dictionaryJobNode);
        // }

        private String operatorToFunctionName(String bstr)
        {
            if (bstr.equals("<"))
            {
                return "LT";
            }
            else if (bstr.equals("<="))
            {
                return "LE";
            }
            else if (bstr.equals(">"))
            {
                return "GT";
            }
            else if (bstr.equals(">="))
            {
                return "GE";
            }
            else if (bstr.equals("=="))
            {
                return "EQ";
            }
            else if (bstr.equals("!="))
            {
                return "NE";
            }
            else if (bstr.equalsIgnoreCase("IS NULL"))
            {
                return "IS_NULL";
            }
            else if (bstr.equalsIgnoreCase("IS NOT NULL"))
            {
                return "IS_NOT_NULL";
            }
            else
            {
                return bstr.toUpperCase();
            }
        }

        private ArrayNode createGenerateExpressionListNode(GenerateExpressionListContext genexprlist)
        {
            ArrayNode genListNode = objMapper.createArrayNode();
            for (GenerateExpressionContext genexpr : genexprlist.generateExpression())
            {
                genListNode.add(createGenerateExpressionNode(genexpr));
            }
            return genListNode;
        }

        private ObjectNode createGenerateExpressionNode(GenerateExpressionContext genexpr)
        {
            ObjectNode genexprNode = objMapper.createObjectNode();
            if (genexpr.as() == null)
            {
                genexprNode.put("col_name", genexpr.expression().getText());
            }
            else
            {
                genexprNode.put("col_name", genexpr.ID().getText());
            }
            genexprNode.put("expression", createExpressionNode(genexpr.expression()));

            return genexprNode;
        }

        private ObjectNode createExpressionNode(ExpressionContext ctx)
        {
            if (ctx.terminalExpression() != null)
            {
                return createTerminalExpressionNode(ctx.terminalExpression());
            }
            else if (ctx.MULDIV() != null)
            {
                String op = ctx.MULDIV().getText();
                String function = op.equals("*") ? "TIMES" : "DIVIDE";

                return packFunctionNode(function, ctx.expression(0), ctx.expression(1));
            }
            else if (ctx.PLUSMINUS() != null)
            {
                String op = ctx.PLUSMINUS().getText();
                String function = op.equals("+") ? "ADD" : "MINUS";

                return packFunctionNode(function, ctx.expression(0), ctx.expression(1));
            }
            else if (ctx.BOOLEANOP() != null)
            {
                String function = operatorToFunctionName(ctx.BOOLEANOP().getText());
                return packFunctionNode(function, ctx.expression(0), ctx.expression(1));
            }
            else if (ctx.ANDOR() != null)
            {
                String function = ctx.ANDOR().getText().toUpperCase();
                return packFunctionNode(function, ctx.expression(0), ctx.expression(1));
            }
            else if (ctx.INOP() != null)
            {
                List<ExpressionContext> inList = ctx.expressionList().expression();
                ExpressionContext[] asArray = inList.toArray(new ExpressionContext[] {});
                ExpressionContext[] args = new ExpressionContext[asArray.length + 1];
                args[0] = ctx.expression(0);
                System.arraycopy(asArray, 0, args, 1, asArray.length);
                return packFunctionNode("IN", args);
            }
            else if (ctx.PRESINGLEOP() != null)
            {
                String function = operatorToFunctionName(ctx.PRESINGLEOP().getText());
                return packFunctionNode(function, ctx.expression(0));
            }
            else if (ctx.POSTSINGLEOP() != null)
            {
                String function = operatorToFunctionName(ctx.POSTSINGLEOP().getText());
                return packFunctionNode(function, ctx.expression(0));
            }
            else if (ctx.uri() != null)
            {
                String function = ctx.uri().getText();
                if (functionAliasMap.containsKey(function))
                    function = functionAliasMap.get(function);

                ExpressionContext[] asArray = null;
                if (ctx.expressionList() != null)
                {
                    List<ExpressionContext> inList = ctx.expressionList().expression();
                    asArray = inList.toArray(new ExpressionContext[] {});
                }
                ObjectNode json = packFunctionNode(function, asArray);

                List<Object> constructorArgs =
                        this.functionCtorArgs.get(ctx.uri().getText());
                ArrayNode constructorArgsNode =
                        createConstructorArgsNode(function, constructorArgs);

                json.put("constructorArgs", constructorArgsNode);

                // Check if this function needs to store files in dist cache
                Function func = FunctionFactory.get(function, constructorArgsNode);

                List<String> cachedFiles = func.getCacheFiles();
                if (cachedFiles != null)
                {
                    this.cachedFiles.addAll(cachedFiles);
                }

                return json;
            }
            else if (ctx.LBRACKET() != null && ctx.expression().size() == 1)
            {
                return createExpressionNode(ctx.expression(0));
            }
            else if (ctx.CASE() != null)
            {
                int numCases = ctx.caseFunctionCallExpression().caseFunctionArg().size();
                ExpressionContext[] args = new ExpressionContext[2 * numCases];
                int idx = 0;
                for (CaseFunctionArgContext caseContext : ctx.caseFunctionCallExpression()
                                                             .caseFunctionArg())
                {
                    args[idx++] = caseContext.expression(0);
                    args[idx++] = caseContext.expression(1);
                }

                return packFunctionNode("CASE", args);
            }
            return null;
        }

        private ObjectNode packFunctionNode(String funcName,
                                            ExpressionContext... expressionArgs)
        {
            ObjectNode node = objMapper.createObjectNode();
            node.put("function", funcName);

            ArrayNode args = objMapper.createArrayNode();
            if (expressionArgs != null)
            {
                for (ExpressionContext expressionArg : expressionArgs)
                {
                    args.add(createExpressionNode(expressionArg));
                }
            }
            node.put("arguments", args);
            return node;
        }

        private ObjectNode createTerminalExpressionNode(TerminalExpressionContext texpr)
        {
            if (texpr.nestedProjectionExpression() != null)
            {
                return createNestedProjectionExpressionNode(texpr.nestedProjectionExpression());
            }
            else if (texpr.columnProjectionExpression() != null)
            {
                return createColumnProjectionExpressionNode(texpr.columnProjectionExpression());
            }
            else if (texpr.constantExpression() != null)
            {
                return createConstantExpressionNode(texpr.constantExpression());
            }
            else if (texpr.mapProjectionExpression() != null)
            {
                return createMapProjectionExpressionNode(texpr.mapProjectionExpression());
            }
            throw new RuntimeException("Unknown type of terminal expression when parsing  "
                    + texpr);
        }

        private ObjectNode createConstantExpressionNode(ConstantExpressionContext constexpr)
        {
            ObjectNode result = objMapper.createObjectNode();

            result.put("function", "CONSTANT");
            ArrayNode argsNode = objMapper.createArrayNode();

            if (constexpr.STRING() != null)
            {
                argsNode.add(CommonUtils.stripQuotes(constexpr.STRING().getText()));
            }
            else if (constexpr.FLOAT() != null)
            {
                String text = constexpr.FLOAT().getText();
                boolean isFloat = text.endsWith("f") || text.endsWith("F");

                if (isFloat)
                    argsNode.add(Float.parseFloat(text.substring(0, text.length() - 1)));
                else
                    argsNode.add(Double.parseDouble(text));

                argsNode.add(isFloat ? "float" : "double");
            }
            else if (constexpr.INT() != null)
            {
                String text = constexpr.INT().getText();
                boolean isLong = text.endsWith("l") || text.endsWith("L");

                if (isLong)
                    argsNode.add(Long.parseLong(text.substring(0, text.length() - 1)));
                else
                    argsNode.add(Integer.parseInt(text));

                argsNode.add(isLong ? "long" : "int");
            }
            else if (constexpr.BOOLEAN() != null)
            {
                argsNode.add(Boolean.parseBoolean(constexpr.BOOLEAN().getText()));
            }

            result.put("arguments", argsNode);
            return result;
        }

        private ObjectNode createNestedProjectionExpressionNode(NestedProjectionExpressionContext nestedexpr)
        {
            List<ColumnProjectionExpressionContext> colexprList =
                    nestedexpr.columnProjectionExpression();
            if (colexprList.size() < 2)
                throw new RuntimeException("Too few arguments in nested column expression");
            ObjectNode toplevelColumn =
                    createColumnProjectionExpressionNode(colexprList.get(0));
            ObjectNode childNode = toplevelColumn;
            ObjectNode resultNode = toplevelColumn;
            for (int i = 1; i < colexprList.size(); i++)
            {
                ColumnProjectionExpressionContext colexpr = colexprList.get(i);
                resultNode = objMapper.createObjectNode();
                resultNode.put("function", "PROJECTION");
                ArrayNode argsNode = objMapper.createArrayNode();
                argsNode.add(childNode);
                // argsNode.add(createProjectionConstantNode(colexpr));
                if (colexpr.ID() != null)
                {
                    argsNode.add(colexpr.ID().getText());
                }
                else
                {
                    argsNode.add(Integer.parseInt(colexpr.INT().getText()));
                }
                resultNode.put("arguments", argsNode);
                childNode = resultNode;

            }
            return resultNode;
        }

        private ObjectNode createProjectionConstantNode(ColumnProjectionExpressionContext columnexpr)
        {
            // create constant object
            ObjectNode constant = objMapper.createObjectNode();
            constant.put("function", "CONSTANT");
            ArrayNode constantArgs = objMapper.createArrayNode();
            constant.put("arguments", constantArgs);

            if (columnexpr.ID() != null)
            {
                constantArgs.add(columnexpr.ID().getText());
            }
            else
            {
                constantArgs.add(Integer.parseInt(columnexpr.INT().getText()));
            }

            return constant;
        }

        private ObjectNode createColumnProjectionExpressionNode(ColumnProjectionExpressionContext columnexpr)
        {
            ObjectNode result = objMapper.createObjectNode();
            result.put("function", "INPUT_PROJECTION");
            ArrayNode argsNode = objMapper.createArrayNode();
            if (columnexpr.ID() != null)
            {
                argsNode.add(columnexpr.ID().getText());
            }
            else
            {
                argsNode.add(Integer.parseInt(columnexpr.INT().getText()));
            }

            result.put("arguments", argsNode);
            return result;

        }

        private ObjectNode createMapProjectionExpressionNode(MapProjectionExpressionContext mapexpr)
        {
            ObjectNode result = objMapper.createObjectNode();
            result.put("function", "MAP_PROJECTION");
            ArrayNode argsNode = objMapper.createArrayNode();

            if (mapexpr.columnProjectionExpression() != null)
                argsNode.add(createColumnProjectionExpressionNode(mapexpr.columnProjectionExpression()));
            else
                argsNode.add(createNestedProjectionExpressionNode(mapexpr.nestedProjectionExpression()));

            argsNode.add(CommonUtils.stripQuotes(mapexpr.STRING().getText()));

            // // create CONSTANT node for the map key
            // ObjectNode constant = objMapper.createObjectNode();
            // constant.put("function", "CONSTANT");
            // ArrayNode constantArgs = objMapper.createArrayNode();
            // constant.put("arguments", constantArgs);
            // constantArgs.add(CommonUtils.stripQuotes(mapexpr.STRING().getText()));
            // argsNode.add(constant);

            result.put("arguments", argsNode);
            return result;
        }

        public ObjectNode createTupleInputNode()
        {
            ObjectNode result = objMapper.createObjectNode();
            result.put("function", "TUPLE_INPUT");
            return result;
        }

        private void emitTypeDefinitions(ObjectNode node,
                                         String fieldName,
                                         TypeDefinitionsContext typeDefinitions)
        {
            boolean first = true;
            StringBuffer typeDefinitionString = new StringBuffer();
            for (TypeDefinitionContext typedColumn : typeDefinitions.typeDefinition())
            {
                if (!first)
                    typeDefinitionString.append(",");
                typeDefinitionString.append(typedColumn.typeString.getText()
                                                                  .toUpperCase()
                        + " "
                        + typedColumn.ID(0));
                first = false;
            }

            node.put(fieldName, typeDefinitionString.toString());
        }

        private void emitCommaSeparatedIDList(StringBuffer resultBuffer,
                                              List<TerminalNode> idlist,
                                              boolean quotedID)
        {
            boolean first = true;
            for (TerminalNode tnode : idlist)
            {
                if (!first)
                    resultBuffer.append(",");
                if (quotedID)
                    resultBuffer.append("\"");
                resultBuffer.append(tnode.getText());
                if (quotedID)
                    resultBuffer.append("\"");
                first = false;
            }
        }

        private ArrayNode createIDListNode(List<TerminalNode> idlist)
        {
            ArrayNode idlistNode = objMapper.createArrayNode();
            for (TerminalNode tnode : idlist)
            {
                idlistNode.add(tnode.getText());
            }
            return idlistNode;
        }

        public String generateIndexName()
        {
            this.indexSequence++;
            return INDEX_PREFIX + indexSequence;
        }

        private String cleanPath(PathContext path)
        {
            return CommonUtils.stripQuotes(path.getText()).trim();
        }

        @Override
        public void exitCreateDictionary(CreateDictionaryContext ctx)
        {
            ObjectNode dict = objMapper.createObjectNode();
            String dictName = ctx.ID().getText();
            for (ColumnDictionaryContext colDict : ctx.columnDictionary())
            {
                String columnName = colDict.ID().getText();
                ArrayNode values = objMapper.createArrayNode();
                for (TerminalNode val : colDict.STRING())
                {
                    values.add(CommonUtils.stripQuotes(val.getText()));
                }
                dict.put(columnName, values);
            }
            inlineDictionaries.put(dictName, dict);
        }

        @Override
        public void exitFunctionDeclaration(@NotNull FunctionDeclarationContext ctx)
        {
            List<Object> ctorArgs = new ArrayList<Object>();

            if (ctx.functionArgs() != null)
            {
                for (ConstantExpressionContext cectx : ctx.functionArgs()
                                                          .constantExpression())
                {
                    if (cectx.BOOLEAN() != null)
                        ctorArgs.add(Boolean.parseBoolean(cectx.BOOLEAN().getText()));
                    else if (cectx.INT() != null)
                        ctorArgs.add(Integer.parseInt(cectx.INT().getText()));
                    else if (cectx.FLOAT() != null)
                        ctorArgs.add(Float.parseFloat(cectx.FLOAT().getText()));
                    else if (cectx.STRING() != null)
                        ctorArgs.add(CommonUtils.stripQuotes(cectx.STRING().getText()).replaceAll("\\\\\\\"", "\""));
                }
            }

            String name = ctx.uri().getText();

            if (ctx.alias != null)
            {
                name = ctx.alias.getText();

                if (functionAliasMap.containsKey(name))
                {
                    throw new IllegalStateException("Function alias [" + name
                            + "] appears more than once in the script.");
                }
                functionAliasMap.put(name, ctx.uri().getText());
            }

            functionCtorArgs.put(name, ctorArgs);
        }

        @Override
        public void exitLoadCachedOperator(LoadCachedOperatorContext ctx)
        {
            operatorNode.put("operator", "LOAD_CACHED_FILE");

            String path = cleanPath(ctx.path());
            path = new Path(path).toString();
            operatorNode.put("path", path);
            operatorNode.put("type", ctx.ID().getText().toUpperCase());
            operatorNode.put("output", operatorCommandLhs);

            ObjectNode paramsNode = objMapper.createObjectNode();
            if (ctx.params() != null)
            {
                for (int i = 0; i < ctx.params().keyval().size(); i++)
                {
                    List<TerminalNode> kv = ctx.params().keyval(i).STRING();
                    paramsNode.put(CommonUtils.stripQuotes(kv.get(0).getText()),
                                   CommonUtils.stripQuotes(kv.get(1).getText()));
                }
            }
            operatorNode.put("params", paramsNode);

            cachedFiles.add(path);
        }

        @Override
        public void exitTeeOperator(TeeOperatorContext ctx)
        {
            operatorNode.put("operator", "TEE");
            operatorNode.put("input", ctx.ID(0).getText());
            operatorNode.put("output", operatorCommandLhs);
            operatorNode.put("path", cleanPath(ctx.path()));
            operatorNode.put("type", ctx.ID(1).getText().toUpperCase());
            operatorNode.put("passthrough", ctx.split == null);

            ObjectNode paramsNode = objMapper.createObjectNode();
            if (ctx.params() != null)
            {
                for (int i = 0; i < ctx.params().keyval().size(); i++)
                {
                    List<TerminalNode> kv = ctx.params().keyval(i).STRING();
                    paramsNode.put(CommonUtils.stripQuotes(kv.get(0).getText()),
                                   CommonUtils.stripQuotes(kv.get(1).getText()));
                }
            }
            operatorNode.put("params", paramsNode);

            if (ctx.generateExpressionList() != null)
            {
                operatorNode.put("generate",
                                 createGenerateExpressionListNode(ctx.generateExpressionList()));
            }

            if (ctx.expression() != null)
            {
                operatorNode.put("filter", createExpressionNode(ctx.expression()));
            }
        }

        @Override
        public void exitSortOperator(@NotNull SortOperatorContext ctx)
        {
            operatorNode.put("operator", "SORT");
            operatorNode.put("output", operatorCommandLhs);
            ArrayNode anode = objMapper.createArrayNode();
            anode.add(ctx.ID().getText());
            operatorNode.put("input", anode);

            anode = objMapper.createArrayNode();

            for (TerminalNode id : ctx.columns().ID())
                anode.add(id.getText());
            operatorNode.put("sortBy", anode);
        }

        @Override
        public void exitCombineOperator(CombineOperatorContext ctx)
        {
            operatorNode.put("operator", "COMBINE");
            operatorNode.put("output", operatorCommandLhs);
            ArrayNode anode = objMapper.createArrayNode();
            for (TerminalNode id : ctx.ID())
                anode.add(id.getText());
            operatorNode.put("input", anode);

            anode = objMapper.createArrayNode();
            for (TerminalNode id : ctx.columns().ID())
                anode.add(id.getText());
            operatorNode.put("pivotBy", anode);
        }

        @Override
        public void exitPivotOperator(PivotOperatorContext ctx)
        {
            operatorNode.put("operator", "PIVOT_BLOCK");
            operatorNode.put("output", operatorCommandLhs);
            operatorNode.put("input", ctx.ID(0).getText());

            ArrayNode anode = objMapper.createArrayNode();
            if (ctx.columns() != null)
            {
                for (TerminalNode id : ctx.columns().ID())
                    anode.add(id.getText());

                operatorNode.put("pivotBy", anode);
            }
            else
            {
                // this is pivot by row or size
                operatorNode.put("pivotType", ctx.type.getText());
                operatorNode.put("pivotValue", ctx.value.getText());
            }
            operatorNode.put("inMemory", ctx.inmemory != null);
        }

        @Override
        public void exitTopNOperator(TopNOperatorContext ctx)
        {
            operatorNode.put("operator", "TOP_N");
            operatorNode.put("output", operatorCommandLhs);
            operatorNode.put("input", ctx.ID().getText());

            ArrayNode grps = objMapper.createArrayNode();
            for (TerminalNode id : ctx.group.ID())
                grps.add(id.getText());
            operatorNode.put("groupBy", grps);

            ArrayNode ords = objMapper.createArrayNode();
            for (TerminalNode id : ctx.order.ID())
                ords.add(id.getText());
            operatorNode.put("orderBy", ords);

            TerminalNode topN = ctx.INT();
            operatorNode.put("topN", topN == null ? 1 : Integer.parseInt(topN.getText()));
        }

        @Override
        public void exitRankOperator(RankOperatorContext ctx)
        {
            operatorNode.put("operator", "RANK");
            operatorNode.put("output", operatorCommandLhs);
            operatorNode.put("input", ctx.inputRelation.getText());
            operatorNode.put("rankAs", ctx.rankColumn.getText());

            ArrayNode grps = objMapper.createArrayNode();
            if (ctx.group != null)
            {
                for (TerminalNode id : ctx.group.ID())
                    grps.add(id.getText());
            }
            operatorNode.put("groupBy", grps);

            ArrayNode ords = objMapper.createArrayNode();
            if (ctx.order != null)
            {
                for (TerminalNode id : ctx.order.ID())
                    ords.add(id.getText());
            }
            operatorNode.put("orderBy", ords);
        }

        @Override
        public void exitGatherOperator(GatherOperatorContext ctx)
        {
            operatorNode.put("operator", "GATHER");
            operatorNode.put("output", operatorCommandLhs);
            ArrayNode anode = objMapper.createArrayNode();
            for (TerminalNode id : ctx.ID())
                anode.add(id.getText());
            operatorNode.put("input", anode);
        }

        @Override
        public void exitBlockgenShuffleCommand(BlockgenShuffleCommandContext ctx)
        {
            shuffleCommandNode =
                    JsonUtils.createObjectNode("type",
                                               "BLOCKGEN",
                                               "name",
                                               ctx.ID(0).getText(),
                                               "blockgenType",
                                               "BY_" + ctx.blockgenType.getText().toUpperCase(),
                                               "partitionKeys",
                                               createIDListNode(ctx.columns().get(0).ID()));

            if (ctx.blockgenValue != null)
                shuffleCommandNode.put("blockgenValue",
                                       Long.parseLong(ctx.blockgenValue.getText()));
            if (ctx.path() != null)
                shuffleCommandNode.put("relation", cleanPath(ctx.path()));

            if (ctx.columns().size() == 2)
            {
                shuffleCommandNode.put("pivotKeys", createIDListNode(ctx.columns()
                                                                        .get(1)
                                                                        .ID()));
            }
            else
            {
                shuffleCommandNode.put("pivotKeys", createIDListNode(ctx.columns()
                                                                        .get(0)
                                                                        .ID()));
            }

            shuffleCommandNode.put("distinct", ctx.distinct != null);

            addLine(ctx, shuffleCommandNode);
        }

        @Override
        public void exitDictionaryShuffleCommand(DictionaryShuffleCommandContext ctx)
        {
            shuffleCommandNode = null;

            if (ctx.columns() != null)
                shuffleCommandNode =
                        JsonUtils.createObjectNode("type",
                                                   "CREATE-DICTIONARY",
                                                   "columns",
                                                   ctx.columns().getText(),
                                                   "name",
                                                   ctx.ID().getText());
            else
                System.err.println("Malformed dictionary job");

            addLine(ctx, shuffleCommandNode);
        }

        @Override
        public void exitUriShuffleCommand(UriShuffleCommandContext ctx)
        {
            ObjectNode paramsNode = objMapper.createObjectNode();
            if (ctx.params() != null)
            {
                for (int i = 0; i < ctx.params().keyval().size(); i++)
                {
                    List<TerminalNode> kv = ctx.params().keyval(i).STRING();
                    paramsNode.put(CommonUtils.stripQuotes(kv.get(0).getText()),
                                   CommonUtils.stripQuotes(kv.get(1).getText()));
                }
            }

            shuffleCommandNode = JsonUtils.createObjectNode("type", ctx.uri().getText(),
                                                            "name", ctx.ID().getText(),
                                                            "args", paramsNode);

            addLine(ctx, shuffleCommandNode);
        }

        private void addLine(ParserRuleContext ctx, JsonNode node)
        {
            String line =
                    input.getText(new Interval(ctx.start.getStartIndex(),
                                               ctx.stop.getStopIndex()));
            ((ObjectNode) node).put("line", line);
        }

        private void deprecation(ParserRuleContext ctx, String msg)
        {
            System.err.println("DEPRECATION: " + msg);
            System.err.println("At: "
                    + input.getText(new Interval(ctx.start.getStartIndex(),
                                                 ctx.stop.getStopIndex())));
        }

        public ArrayNode createConstructorArgsNode(String functionName,
                                                   List<Object> constructorArgs)
        {
            ArrayNode constructorArgsNode = null;
            constructorArgsNode = objMapper.createArrayNode();
            if (constructorArgs != null && !constructorArgs.isEmpty())
            {
                for (Object constructorArg : constructorArgs)
                {
                    if (constructorArg instanceof Boolean)
                        constructorArgsNode.add((boolean) (Boolean) constructorArg);
                    else if (constructorArg instanceof Integer)
                        constructorArgsNode.add((int) (Integer) constructorArg);
                    else if (constructorArg instanceof Float)
                        constructorArgsNode.add((float) (Float) constructorArg);
                    else if (constructorArg instanceof String)
                        constructorArgsNode.add((String) constructorArg);
                    else
                        throw new RuntimeException(String.format("%s UDF cannot have [%s] of type %s as constructor argument",
                                                                 functionName,
                                                                 constructorArg,
                                                                 constructorArg.getClass()));
                }
            }
            return constructorArgsNode;
        }

        private JsonNode createGroupingSetsNode(GroupingSetsClauseContext groupingSetsClause)
        {
            ArrayNode groupingSetsNode = objMapper.createArrayNode();
            for (CuboidContext cuboidContext : groupingSetsClause.cuboid())
            {
                StringBuffer cuboidBuffer = new StringBuffer();
                emitCommaSeparatedIDList(cuboidBuffer,
                                         cuboidContext.columns().ID(),
                                         false);
                groupingSetsNode.add(cuboidBuffer.toString());
            }
            return groupingSetsNode;
        }

        private <T> List<List<T>> combinations(List<T> elements, int level, int idx)
        {
            List<List<T>> combos = new LinkedList<List<T>>();
            for (int i = idx; i < elements.size(); i++)
            {
                List<T> leaf = new LinkedList<T>();
                leaf.add(elements.get(i));
                combos.add(leaf);

                if (level == 1 || i == elements.size() - 1)
                    continue;

                List<List<T>> recCombos = combinations(elements, level - 1, i + 1);
                for (List<T> recCombo : recCombos)
                {
                    recCombo.add(elements.get(i));
                    combos.add((recCombo));
                }
            }
            return combos;
        }

        private JsonNode createGroupingCombosNode(int comboLevels, List<TerminalNode> id)
        {
            ArrayNode comboGSNode = objMapper.createArrayNode();
            // Explicitly add the empty node for complete rollup.
            comboGSNode.add("");

            for (List<TerminalNode> group : combinations(id, comboLevels, 0))
            {
                StringBuffer gsBuffer = new StringBuffer();
                emitCommaSeparatedIDList(gsBuffer, group, false);
                comboGSNode.add(gsBuffer.toString());
            }

            return comboGSNode;
        }

        /**
         * Given a list of elements <code> { a, b, c } </code>, return a rollup set
         * <code> { {}, { a }, { a, b } , { a, b, c } ] </code>
         *
         * @param group
         * @return
         */
        private <T> List<List<T>> createOneRollup(List<T> group)
        {
            List<List<T>> rollupList = new LinkedList<List<T>>();
            rollupList.add(new LinkedList<T>());

            List<T> base = null;
            for (T column : group)
            {
                List<T> thisRollup = new LinkedList<T>();
                if (base != null)
                    thisRollup.addAll(base);

                thisRollup.add(column);
                rollupList.add(thisRollup);
                base = thisRollup;
            }

            return rollupList;
        }

        /**
         * Given two lists of lists
         *
         * <pre>
         * A := { { a } { b, c } }
         * B := { { d } { e } }
         * A x B := { { a, d } { a, e } { b, c, d } { b, c, e } }
         * </pre>
         *
         * @param leftOperand
         * @param rightOperand
         * @return
         */
        private <T> List<List<T>> multiply(List<List<T>> leftOperand,
                                           List<List<T>> rightOperand)
        {
            if (leftOperand == null)
                return rightOperand;

            List<List<T>> product = new LinkedList<List<T>>(leftOperand);

            for (List<T> left : leftOperand)
            {
                for (List<T> right : rightOperand)
                {
                    // special case -- ignore right side empty list during product to
                    // avoid duplicates
                    if (right.isEmpty())
                        continue;

                    List<T> partialProduct = new LinkedList<T>(left);
                    partialProduct.addAll(right);
                    product.add(partialProduct);
                }
            }

            return product;
        }

        private JsonNode createRollupsNode(RollupsClauseContext rollupsClause)
        {
            List<List<TerminalNode>> growthList = null;
            for (CuboidContext cuboidContext : rollupsClause.cuboid())
                growthList =
                        multiply(growthList,
                                 createOneRollup(cuboidContext.columns().ID()));

            ArrayNode rollupSetsNode = objMapper.createArrayNode();
            // Explicitly add the empty node for complete rollup.
            rollupSetsNode.add("");

            for (List<TerminalNode> group : growthList)
            {
                // disregard intermediate rollups
                if (group.isEmpty())
                    continue;

                StringBuffer cuboidBuffer = new StringBuffer();
                emitCommaSeparatedIDList(cuboidBuffer, group, false);
                rollupSetsNode.add(cuboidBuffer.toString());
            }
            growthList = null;

            return rollupSetsNode;
        }

        private void parseCubeStatement(CubeStatementContext ctx, ObjectNode json)
        {
            json.put("operator", "CUBE");
            json.put("input", ctx.ID().getText());
            json.put("dimensions", createIDListNode(ctx.outer.ID()));
            if (ctx.inner != null)
                json.put("innerDimensions", createIDListNode(ctx.inner.ID()));

            // Generate grouping sets for cube operator
            if (ctx.groupingSetsClause() != null)
                json.put("groupingSets", createGroupingSetsNode(ctx.groupingSetsClause()));
            else if (ctx.groupingCombosClause() != null)
                json.put("groupingSets",
                         createGroupingCombosNode(Integer.parseInt(ctx.groupingCombosClause().n.getText()),
                                                  ctx.outer.ID()));
            else if (ctx.rollupsClause() != null)
                json.put("groupingSets", createRollupsNode(ctx.rollupsClause()));

            if (ctx.htsize != null)
                json.put("hashTableSize", Integer.parseInt(ctx.htsize.getText()));

            ArrayNode aggregates = objMapper.createArrayNode();
            json.put("aggregates", aggregates);

            for (CubeAggregateContext cac : ctx.cubeAggregateList().cubeAggregate())
            {
                ArrayNode inputs =
                        cac.parameters == null ? null
                                : createIDListNode(cac.parameters.ID());
                String output = cac.ID().getText();

                ObjectNode aggNode =
                        JsonUtils.createObjectNode("input", inputs, "output", output);

                // if this is a simple aggregator (ID or uri)
                if (cac.cubeAggregationFunction().aggregationFunction() != null)
                {
                    AggregationFunctionContext afc =
                            cac.cubeAggregationFunction().aggregationFunction();

                    String type = null;

                    if (afc.uri() != null)
                    {
                        type = afc.uri().getText();
                    }
                    else
                    {
                        type = afc.ID().getText();
                        // if ID, check if this is an alias to uri
                        if (functionAliasMap.containsKey(type))
                            type = functionAliasMap.get(type);
                    }

                    aggNode.put("type", type);

                    // check if there are constructor args for this aggregator
                    if (functionCtorArgs.containsKey(type))
                    {
                        aggNode.put("constructorArgs",
                                    createConstructorArgsNode(type,
                                                              functionCtorArgs.get(type)));
                    }
                }
                // if this is dual aggregator [ID, ID]
                else
                {
                    ArrayNode type =
                            createIDListNode(cac.cubeAggregationFunction()
                                                .cubePartitionedAdditiveAggFunction()
                                                .ID());
                    aggNode.put("type", type);
                }

                aggregates.add(aggNode);
            }
        }

        @Override
        public void exitCubeOperator(CubeOperatorContext ctx)
        {
            parseCubeStatement(ctx.cubeStatement(), this.operatorNode);

            operatorNode.put("output", this.operatorCommandLhs);
        }

        @Override
        public void exitCubeShuffleCommand(CubeShuffleCommandContext ctx)
        {
            shuffleCommandNode = this.objMapper.createObjectNode();
            parseCubeStatement(ctx.cubeStatement(), this.shuffleCommandNode);

            // rename "operator" to "type
            shuffleCommandNode.put("type", shuffleCommandNode.get("operator"));
            shuffleCommandNode.remove("operator");

            // rename "input" to "name"
            shuffleCommandNode.put("name", shuffleCommandNode.get("input"));
            shuffleCommandNode.remove("input");

            addLine(ctx, shuffleCommandNode);

        }

        @Override
        public void exitDistinctShuffleCommand(DistinctShuffleCommandContext ctx)
        {
            shuffleCommandNode = this.objMapper.createObjectNode();
            shuffleCommandNode.put("name", ctx.ID().getText());
            shuffleCommandNode.put("type", "DISTINCT");

            addLine(ctx, shuffleCommandNode);
        }

        @Override
        public void exitJoinShuffleCommand(@NotNull JoinShuffleCommandContext ctx)
        {
            shuffleCommandNode = this.objMapper.createObjectNode();
            shuffleCommandNode.put("type", "JOIN");
            shuffleCommandNode.put("name", ctx.ID().getText());
            shuffleCommandNode.put("joinKeys", createIDListNode(ctx.columns(0).ID()));

            if (ctx.joinType() != null)
                shuffleCommandNode.put("joinType", ctx.joinType().getText().toUpperCase());

            if (ctx.columns().size() > 1)
            {
                shuffleCommandNode.put("partitionKeys", createIDListNode(ctx.columns(1).ID()));
            }

            addLine(ctx, shuffleCommandNode);
        }

        @Override
        public void exitUriOperator(UriOperatorContext ctx)
        {
            String classname = ctx.uri().getText();

            // check if there are constructor args
            List<Object> constructorArgs = functionCtorArgs.get(classname);

            ArrayNode constructorArgsNode =
                    createConstructorArgsNode(classname, constructorArgs);
            if (constructorArgs != null)
                operatorNode.put("constructorArgs", constructorArgsNode);

            // check if this is an alias name
            if (functionAliasMap.containsKey(classname))
                classname = functionAliasMap.get(classname);

            Object object = null;
            try
            {
                object =
                        FunctionFactory.createFunctionObject(classname,
                                                             constructorArgsNode);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            if (object instanceof TupleOperator)
                operatorNode.put("operator", "USER_DEFINED_TUPLE_OPERATOR");
            else if (object instanceof BlockOperator)
                operatorNode.put("operator", "USER_DEFINED_BLOCK_OPERATOR");
            else
                throw new RuntimeException(classname
                        + " should implement TupleOperator or BlockOperator interface");

            operatorNode.put("class", classname);
            operatorNode.put("input", createIDListNode(ctx.idlist().ID()));
            operatorNode.put("output", operatorCommandLhs);

            ObjectNode paramsNode = objMapper.createObjectNode();
            if (ctx.params() != null)
            {
                for (int i = 0; i < ctx.params().keyval().size(); i++)
                {
                    List<TerminalNode> kv = ctx.params().keyval(i).STRING();
                    paramsNode.put(CommonUtils.stripQuotes(kv.get(0).getText()),
                                   CommonUtils.stripQuotes(kv.get(1).getText()));
                }
            }
            operatorNode.put("args", paramsNode);

            // check if this operator want to cache files
            if (object instanceof NeedCachedFiles)
            {
                List<String> paths = ((NeedCachedFiles) object).getCachedFiles();
                if (paths != null)
                    this.cachedFiles.addAll(paths);
            }

            // check if this operator wants to cache an index
            if (object instanceof IndexCacheable)
            {
                List<String> paths = ((IndexCacheable) object).getCachedIndices();
                ArrayNode indexNameJson = objMapper.createArrayNode();
                for (String path : paths)
                {
                    String indexName = generateIndexName();

                    ObjectNode cacheIndex = objMapper.createObjectNode();
                    cacheIndex.put("name", indexName);
                    cacheIndex.put("path", path);
                    cacheIndexNode.add(cacheIndex);

                    indexNameJson.add(indexName);
                }

                operatorNode.put("index", indexNameJson);
            }
        }

        @Override
        public void exitOnCompletionTasks(OnCompletionTasksContext ctx)
        {
            ArrayNode tasks = objMapper.createArrayNode();

            for (OnCompletionTaskContext taskCtx : ctx.onCompletionTask())
            {
                ObjectNode task = objMapper.createObjectNode();
                ArrayNode args = objMapper.createArrayNode();
                task.put("paths", args);

                if (taskCtx.rmTask() != null)
                {
                    task.put("type", "rm");
                    for (PathContext path : taskCtx.rmTask().path())
                        args.add(CommonUtils.stripQuotes(path.getText()));
                }
                else if (taskCtx.mvTask() != null)
                {
                    task.put("type", "mv");

                    for (PathContext path : taskCtx.mvTask().path())
                        args.add(CommonUtils.stripQuotes(path.getText()));
                }
                else if (taskCtx.mkdirTask() != null)
                {
                    task.put("type", "mkdir");

                    for (PathContext path : taskCtx.mkdirTask().path())
                        args.add(CommonUtils.stripQuotes(path.getText()));
                }
                else if (taskCtx.dumpTask() != null)
                {
                    task.put("type", "dump");
                    args.add(CommonUtils.stripQuotes(taskCtx.dumpTask().path().getText()));
                }
                else if (taskCtx.uriTask() != null)
                {
                    task.put("type", taskCtx.uriTask().uri().getText());

                    for (PathContext path : taskCtx.uriTask().path())
                        args.add(CommonUtils.stripQuotes(path.getText()));
                }

                tasks.add(task);
            }

            programNode.put("onCompletion", tasks);
        }

    }
}
