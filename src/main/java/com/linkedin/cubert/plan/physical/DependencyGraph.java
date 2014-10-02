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

package com.linkedin.cubert.plan.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import com.linkedin.cubert.utils.print;

/**
 * Generates the execution plan of a DAG of dependencies, using topological sort.
 * 
 * Dependencies are added via the {@code addNode} method, and the plan is obtained via the
 * {@code getSerialPlan} method.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DependencyGraph
{
    static enum Color
    {
        WHITE, GRAY, BLACK
    }

    static final class GraphNode
    {
        final String name;
        final JsonNode json;
        final List<String> parents = new ArrayList<String>();
        final List<GraphNode> children = new ArrayList<GraphNode>();
        Color color = Color.WHITE;

        GraphNode(String name, List<String> parents, JsonNode json)
        {
            this.name = name;
            this.json = json;
            if (parents != null)
                this.parents.addAll(parents);
        }
    }

    private final Map<String, GraphNode> graphNodes = new HashMap<String, GraphNode>();

    public void addNode(String name, List<String> parents, JsonNode json)
    {
        if (graphNodes.containsKey(name))
        {
            throw new IllegalArgumentException("Node [" + name + "] already exists");
        }

        GraphNode node = new GraphNode(name, parents, json);
        graphNodes.put(name, node);
    }

    public List<JsonNode> getSerialPlan()
    {
        List<JsonNode> plan = new ArrayList<JsonNode>();
        Set<String> whiteNodes = new HashSet<String>();
        List<String> inputNodes = new ArrayList<String>();

        setChildren();

        // initialize state
        for (GraphNode node : graphNodes.values())
        {
            if (node.parents.isEmpty())
            {
                inputNodes.add(node.name);
            }
            else
            {
                whiteNodes.add(node.name);
            }
            node.color = Color.WHITE;
        }

        // first visit all input nodes
        for (String inputNode : inputNodes)
        {
            visit(inputNode, whiteNodes, plan);
        }

        // visit other nodes
        while (!whiteNodes.isEmpty())
        {
            String next = whiteNodes.iterator().next();
            visit(next, whiteNodes, plan);
        }

        Collections.reverse(plan);
        return plan;
    }

    private void visit(String inputNode, Set<String> whiteNodes, List<JsonNode> plan)
    {
        GraphNode node = graphNodes.get(inputNode);
        if (node.color == Color.GRAY)
        {
            throw new IllegalStateException("Cannot create plan for graph with cyclic dependency");
        }

        if (node.color == Color.WHITE)
        {
            node.color = Color.GRAY;
            for (GraphNode child : node.children)
            {
                visit(child.name, whiteNodes, plan);
            }
            node.color = Color.BLACK;
            whiteNodes.remove(node.name);
            plan.add(node.json);
        }
    }

    public void setChildren()
    {
        // clear out children list first
        for (GraphNode node : graphNodes.values())
        {
            node.children.clear();
        }

        for (GraphNode node : graphNodes.values())
        {
            for (String parent : node.parents)
            {
                GraphNode parentNode = graphNodes.get(parent);
                if (parentNode == null)
                    print.f("parent is null for %s %s", node.name, parent);
                parentNode.children.add(node);
            }
        }
    }

    public boolean hasUnfinishedJobs()
    {
        for (GraphNode node : graphNodes.values())
        {
            if (node.color == Color.WHITE || node.color == Color.GRAY)
                return true;
        }
        return false;
    }

    public List<JsonNode> getReadyJobs()
    {
        List<JsonNode> readyJobs = new ArrayList<JsonNode>();
        for (GraphNode node : graphNodes.values())
        {
            if (readyForScheduling(node))
            {
                readyJobs.add(node.json);
            }
        }
        return readyJobs;
    }

    private boolean readyForScheduling(GraphNode node)
    {
        if (node.color == Color.WHITE)
        {
            for (String parent : node.parents)
            {
                GraphNode parentNode = graphNodes.get(parent);
                if (parentNode.color == Color.WHITE || parentNode.color == Color.GRAY)
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void setJobToScheduled(JsonNode job)
    {
        String jobName = job.get("name").getTextValue();
        GraphNode inProgressJob = graphNodes.get(jobName);
        inProgressJob.color = Color.GRAY;
    }

    public void setJobToFinished(JsonNode job)
    {
        String jobName = job.get("name").getTextValue();
        GraphNode finishedJob = graphNodes.get(jobName);
        finishedJob.color = Color.BLACK;
    }

    public String prettyPrint(List<String> jobNames)
    {
        String retVal = "\nDependency graph\n";
        for (String name : jobNames)
        {
            List<String> nodes = graphNodes.get(name).parents;
            if (nodes.size() == 0)
            {
                retVal += (name + " is independent\n");
            }
            else
            {
                String listOfNames = "";
                for (String parentName : nodes)
                {
                    listOfNames += (parentName + ", ");
                }
                retVal +=
                        (name + " depends on "
                                + listOfNames.substring(0, listOfNames.length() - 2) + "\n");
            }
        }
        return retVal;
    }

    public static void main(String[] args)
    {
        DependencyGraph g = new DependencyGraph();

        JsonNodeFactory nc = JsonNodeFactory.instance;

        JsonNode a = nc.numberNode(1);
        JsonNode b = nc.numberNode(2);
        JsonNode c = nc.numberNode(3);
        JsonNode d = nc.numberNode(4);
        JsonNode e = nc.numberNode(5);
        JsonNode f = nc.numberNode(6);
        JsonNode h = nc.numberNode(7);
        JsonNode i = nc.numberNode(8);

        g.addNode("input", null, a);
        g.addNode("loaddict", null, b);
        g.addNode("second", null, c);
        g.addNode("encode", Arrays.asList(new String[] { "input", "loaddict" }), d);
        g.addNode("groupby", Arrays.asList(new String[] { "encode" }), e);
        g.addNode("filter", Arrays.asList(new String[] { "groupby" }), f);
        g.addNode("join", Arrays.asList(new String[] { "filter", "second" }), h);
        g.addNode("shuffle", Arrays.asList(new String[] { "join" }), i);
        System.out.println(g.getSerialPlan());
    }
}
