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
import com.linkedin.cubert.analyzer.physical.LineageGraph.LineageGraphVertex;
import com.linkedin.cubert.analyzer.physical.Lineage.*;

public class ColumnLineage extends LineageGraphVertex
{
    public OutputColumn node;
    public ArrayList<LineageGraphVertex> childNodes = new ArrayList<LineageGraphVertex>();
    public ArrayList<LineageGraphVertex> parentNodes =
            new ArrayList<LineageGraphVertex>();
    private String nodeType = null;

    public ColumnLineage(OutputColumn sourceColumn, String nodeType)
    {
        this.node = sourceColumn;
        this.nodeType = nodeType;
    }

    @Override
    public List<LineageGraphVertex> getChildVertices()
    {
        return childNodes;
    }

    @Override
    public List<LineageGraphVertex> getParentVertices()
    {
        return parentNodes;
    }

    @Override
    public String getNodeType()
    {
        return nodeType;
    }

    // Returns true, if this column was computed as an expression on multiple columns
    // from the same source.
    public boolean isExpressionOutput()
    {
        HashMap<ObjectNode, String> sourceOp2Col = new HashMap<ObjectNode, String>();
        for (LineageGraphVertex parent : parentNodes)
        {
            ColumnLineage parentLineage = (ColumnLineage) parent;
            if (sourceOp2Col.containsKey(parentLineage.node.opNode)
                    && !sourceOp2Col.get(parentLineage.node.opNode)
                                    .equals(parentLineage.node.columnName))
                return true;
            sourceOp2Col.put(parentLineage.node.opNode, parentLineage.node.columnName);
        }
        return false;
    }

    public String toString()
    {
        return ("ColumnLineage = " + this.node.toString());
    }

}
