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
import java.lang.InstantiationException;
import java.lang.IllegalAccessException;

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

public class SummaryRewriter implements PlanRewriter
{

    private List<Class<? extends AggregateRewriter>> aggregateRewriters =
            new ArrayList<Class<? extends AggregateRewriter>>();

    public SummaryRewriter()
    {
        aggregateRewriters.add(CountDistinctRewriter.class);
    }

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit)
    {
        for (Class<? extends AggregateRewriter> rewriterClass : aggregateRewriters)
        {
            try
            {
                AggregateRewriter aggRewriter = rewriterClass.newInstance();
                plan = aggRewriter.rewrite(plan);
            }
            catch (InstantiationException e)
            {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }

        }
        return plan;
    }

}
