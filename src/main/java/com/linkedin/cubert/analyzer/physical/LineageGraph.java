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

public class LineageGraph
{
    public static abstract class LineageGraphVertex
    {

        public List<LineageGraphVertex> getEdgeVertices(boolean isForward)
        {
            if (isForward)
                return getChildVertices();
            else
                return getParentVertices();
        }

        public abstract List<LineageGraphVertex> getChildVertices();

        public abstract List<LineageGraphVertex> getParentVertices();

        public abstract String getNodeType();

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            return false;
        }
    }

    public static interface LineageGraphVisitor
    {
        public boolean visit(LineageGraphVertex graphNode);

        public void finishSubtree(LineageGraphVertex graphNode);
    }

    public static void visitLineageGraph(LineageGraphVertex graphNode,
                                         LineageGraphVisitor graphVisitor,
                                         boolean isForward)
    {
        ArrayList<Pair<LineageGraphVertex, LineageGraphVertex>> qnodes =
                new ArrayList<Pair<LineageGraphVertex, LineageGraphVertex>>();

        // hash set of in progress vertices
        HashSet<LineageGraphVertex> inProgressNodes = 
          new HashSet<LineageGraphVertex>();

        qnodes.add(new Pair<LineageGraphVertex, LineageGraphVertex>(null, graphNode));

        visitLineageGraph(qnodes, graphVisitor, isForward, inProgressNodes);
    }

    private static boolean visitLineageGraph(ArrayList<Pair<LineageGraphVertex, LineageGraphVertex>> qnodes,
                                             LineageGraphVisitor graphVisitor,
                                             boolean isForward,
                                             Set<LineageGraphVertex> inProgressNodes)
    {
        if (qnodes.size() == 0)
            return false;
        Pair<LineageGraphVertex, LineageGraphVertex> edge = qnodes.remove(0);
        LineageGraphVertex snode = edge.getSecond();

        // add to in progress
        inProgressNodes.add(snode);
        if (true)
        {
            LineageHelper.trace("Graph Visit Edge\n**************");
            LineageHelper.trace("Parent vertex\n**************");
            if (edge.getFirst() != null)
                LineageHelper.trace(edge.getFirst().toString() + "\n**********");
            LineageHelper.trace("Child vertex\n*************");
            LineageHelper.trace(snode.toString() + "\n**************");
        }

        if (!graphVisitor.visit(snode))
            return false;

        // when the node is a terminal node with no outgoing edges, it is
        // considered subtree complete.
        if (snode.getEdgeVertices(isForward) == null)
          completeSubtree(graphVisitor, snode, isForward, inProgressNodes);

        for (LineageGraphVertex v : snode.getEdgeVertices(isForward))
        {
            qnodes.add(new Pair<LineageGraphVertex, LineageGraphVertex>(snode, v));
        }

        visitLineageGraph(qnodes, graphVisitor, isForward, inProgressNodes);
        return true;

    }

    private static void completeSubtree(LineageGraphVisitor graphVisitor,
                                        LineageGraphVertex completedNode,
                                        boolean isForward,
                                        Set<LineageGraphVertex> inProgressNodes)
    {
      graphVisitor.finishSubtree(completedNode);
 
      inProgressNodes.remove(completedNode);
      for (LineageGraphVertex v : completedNode.getEdgeVertices(!isForward))
        checkCompletion(graphVisitor, v, isForward, inProgressNodes);
    }

    private static void checkCompletion(LineageGraphVisitor graphVisitor,
                                        LineageGraphVertex node,
                                        boolean isForward,
                                        Set<LineageGraphVertex> inProgressNodes)
    {
      // for all outgoing edges, if the end point is no longer preset in 
      // inProgressNodes, the subtree is complete.
      for (LineageGraphVertex v : node.getEdgeVertices(isForward)){
        if (inProgressNodes.contains(v))
          return;
      }
      completeSubtree(graphVisitor, node, isForward, inProgressNodes);
    }

    public static class TerminalNodeTracer implements LineageGraphVisitor
    {

        public ArrayList<LineageGraphVertex> matchedNodes =
                new ArrayList<LineageGraphVertex>();
        private List<String> nodeTypes = null;

        public TerminalNodeTracer(String[] ntypes)
        {
            if (ntypes != null)
                this.nodeTypes = Arrays.asList(ntypes);
        }

        public boolean visit(LineageGraphVertex snode)
        {
            LineageHelper.trace("Terminal node tracer visiting " + snode.toString());
            if (nodeTypes == null || nodeTypes.size() == 0
                    || this.nodeTypes.indexOf(snode.getNodeType()) != -1)
                matchedNodes.add(snode);
            return true;
        }

        @Override
        public void finishSubtree(LineageGraphVertex graphNode)
        {

        }

    }

    public static class LineagePath implements Cloneable
    {
        public ArrayList<LineageGraphVertex> nodes = new ArrayList<LineageGraphVertex>();

        public LineagePath clone()
        {
            LineagePath result = new LineagePath();
            result.nodes.addAll(this.nodes);
            return result;
        }

        public String toString()
        {
            return CommonUtils.listAsString(nodes, "\n");
        }

    }

    public static class PathMatcher implements LineageGraphVisitor
    {
        // partial match maintained for each level which is a prefix of the matching
        // pattern. All possible partial matches are maintained
        public ArrayList<ArrayList<LineagePath>> partialMatchedPaths;
        public ArrayList<LineagePath> matchedPaths;
        public ArrayList<String> pathNodeTypeSequence;
        public LineageGraphVertex terminalNode = null;

        public PathMatcher(ArrayList<String> pathNodeTypeSequence,
                           LineageGraphVertex terminalNode)
        {
            this.pathNodeTypeSequence = pathNodeTypeSequence;
            this.partialMatchedPaths =
                    new ArrayList<ArrayList<LineagePath>>(pathNodeTypeSequence.size());
            for (int i = 0; i < pathNodeTypeSequence.size(); i++)
                this.partialMatchedPaths.add(new ArrayList<LineagePath>());
            this.matchedPaths = new ArrayList<LineagePath>();
            this.terminalNode = terminalNode;
        }

        @Override
        public boolean visit(LineageGraphVertex graphNode)
        {
            // for all partial matches, see if the match can be extended into next bucket.
            LineageHelper.trace("PathMatcher visiting graphNode " + graphNode.toString());
            for (int i = 0; i < pathNodeTypeSequence.size(); i++)
            {

                if (!graphNode.getNodeType().equals(this.pathNodeTypeSequence.get(i)))
                    continue;

                // if no partial match at the current level.
                if ((i) > 0 && partialMatchedPaths.get(i - 1) == null)
                    continue;

                if (i == 0)
                {
                    LineagePath p = new LineagePath();
                    p.nodes.add(graphNode);
                    partialMatchedPaths.get(i).add(p);
                    continue;
                }

                for (LineagePath p : partialMatchedPaths.get(i - 1))
                {
                    LineagePath pExt = p.clone();
                    pExt.nodes.add(graphNode);
                    partialMatchedPaths.get(i).add(pExt);

                    if (i == pathNodeTypeSequence.size() - 1 && terminalNode == null)
                        this.matchedPaths.add(pExt);
                }

            }

            // If matching a valid terminalNode,
            if (terminalNode == null || graphNode != terminalNode)
                return true;
            int lb = pathNodeTypeSequence.size() - 1;
            for (LineagePath p : partialMatchedPaths.get(lb))
            {
                LineagePath pExt = p.clone();
                pExt.nodes.add(graphNode);
                this.matchedPaths.add(pExt);
                LineageHelper.trace("Found matching lineage path = " + pExt.toString());
            }
            return true;
        }

        @Override
        public void finishSubtree(LineageGraphVertex graphNode)
        {
            // all partially matched paths ending at graphNode are deleted.
            for (int i = 0; i < partialMatchedPaths.size(); i++)
            {
                ArrayList<LineagePath> pathList = partialMatchedPaths.get(i);
                ArrayList<LineagePath> deleteList = new ArrayList<LineagePath>();
                for (LineagePath p : pathList)
                {
                    int li = p.nodes.size() - 1;
                    if (p.nodes.get(li) == graphNode)
                        deleteList.add(p);
                }
                partialMatchedPaths.get(i).removeAll(deleteList);
            }
        }

    }

    public static List<LineageGraphVertex> traceTerminalNodes(LineageGraphVertex startNode,
                                                              String[] terminalNodeTypes,
                                                              boolean isForward)
    {
        LineageHelper.trace("Tracing terminal Nodes for nodeTypes = "
                + (terminalNodeTypes != null ? Arrays.toString(terminalNodeTypes)
                        : "EMPTY"));
        TerminalNodeTracer tracer = new TerminalNodeTracer(terminalNodeTypes);
        visitLineageGraph(startNode, tracer, isForward);
        return tracer.matchedNodes;
    }

    public static List<LineagePath> traceMatchingPaths(LineageGraphVertex startVertex,
                                                       ArrayList<String> nodeTypes,
                                                       LineageGraphVertex terminalVertex,
                                                       boolean isForward)
    {

        PathMatcher pathMatcher = new PathMatcher(nodeTypes, terminalVertex);
        LineageHelper.trace("Trace matching paths for "
                + CommonUtils.listAsString(nodeTypes));
        visitLineageGraph(startVertex, pathMatcher, isForward);
        int li = nodeTypes.size() - 1;
        return pathMatcher.matchedPaths;
    }

    public static class PathTracer implements LineageGraphVisitor
    {
        private LineageGraphVertex endVertex;
        private ArrayList<LineagePath> pathList;
        private LineageGraphVertex startVertex;

        public PathTracer(LineageGraphVertex startVertex, LineageGraphVertex endVertex)
        {
            this.endVertex = endVertex;
            this.startVertex = startVertex;
            this.pathList = new ArrayList<LineagePath>();

            LineagePath singlePath = new LineagePath();
            singlePath.nodes.add(startVertex);
            this.pathList.add(singlePath);

        }

        @Override
        public boolean visit(LineageGraphVertex graphNode)
        {
            for (LineagePath lpath : this.pathList)
            {
                int size = lpath.nodes.size();
                List<LineageGraphVertex> parentList = graphNode.getParentVertices();
                if (CommonUtils.indexOfByRef(parentList, lpath.nodes.get(size - 1)) != -1)
                    lpath.nodes.add(graphNode);
            }
            if (this.pathList.size() == 0)
            {

            }
            if (graphNode == endVertex)
                return false;
            return true;
        }

        @Override
        public void finishSubtree(LineageGraphVertex graphNode)
        {

        }
    }

    public static LineagePath tracePath(LineageGraphVertex startVertex,
                                        LineageGraphVertex endVertex)
    {
        PathTracer pathTracer = new PathTracer(startVertex, endVertex);
        visitLineageGraph(startVertex, pathTracer, true);
        for (LineagePath lPath : pathTracer.pathList)
        {
            if (lPath.nodes.size() > 0
                    && lPath.nodes.get(lPath.nodes.size() - 1) == endVertex)
                return lPath;
        }
        return null;
    }

    public static List<LineageGraphVertex> traceAllReachable(LineageGraphVertex startNode,
                                                             boolean isForward)
    {
        return traceTerminalNodes(startNode, null, isForward);
    }

}
