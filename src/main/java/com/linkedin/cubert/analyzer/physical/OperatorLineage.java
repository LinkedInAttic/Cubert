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

public class OperatorLineage extends LineageGraphVertex
{
    public ArrayList<LineageGraphVertex> childNodes = new ArrayList<LineageGraphVertex>();
    public ArrayList<LineageGraphVertex> parentNodes =
            new ArrayList<LineageGraphVertex>();
    public ObjectNode node;
    public String nodeType;

    public OperatorLineage(ObjectNode sourceNode, String nodeType)
    {
        this.node = sourceNode;
        this.nodeType = nodeType;
    }

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

    public String toString()
    {
        return ("Operator Lineage = " + node.toString());
    }

}
