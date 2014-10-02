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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.cubert.plan.physical.DependencyGraph;
import com.linkedin.cubert.utils.print;

public class TestDependencyGraph
{
    static class GraphCreator
    {
        Map<String, List<String>> edges = new HashMap<String, List<String>>();
        Set<String> vertices = new HashSet<String>();

        void addDependency(int parent, int depends)
        {
            String from = Integer.toString(depends);
            String to = Integer.toString(parent);
            vertices.add(to);
            vertices.add(from);

            List<String> list = edges.get(from);
            if (list == null)
            {
                list = new ArrayList<String>();
                edges.put(from, list);
            }

            list.add(to);
        }

        DependencyGraph getGraph()
        {
            DependencyGraph g = new DependencyGraph();
            ObjectMapper mapper = new ObjectMapper();

            for (String vertex : vertices)
            {
                ObjectNode json = mapper.createObjectNode();
                json.put("name", vertex);
                List<String> parents = edges.get(vertex);
                g.addNode(vertex, parents, json);
            }

            return g;
        }

        void assertPlan()
        {
            List<JsonNode> plan = getGraph().getSerialPlan();

            Assert.assertEquals(plan.size(), vertices.size());

            Set<String> activated = new HashSet<String>();

            for (JsonNode node : plan)
            {
                String name = node.get("name").getTextValue();
                List<String> parents = edges.get(name);
                print.f("Testing %s. parents=%d",
                        name,
                        parents == null ? 0 : parents.size());
                if (parents != null)
                {
                    for (String parent : parents)
                    {
                        Assert.assertTrue(activated.contains(parent));
                    }
                }
                activated.add(name);
            }
        }
    }

    @Test
    public void testLinearGraph()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 2);
        g.addDependency(2, 3);
        g.addDependency(3, 4);
        g.addDependency(4, 5);

        g.assertPlan();
    }

    @Test
    public void testDiamond()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 2);
        g.addDependency(1, 3);
        g.addDependency(3, 4);
        g.addDependency(2, 4);

        g.assertPlan();
    }

    @Test
    public void testEmbeddedDiamond()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 2);
        g.addDependency(1, 5);
        g.addDependency(2, 3);
        g.addDependency(2, 4);
        g.addDependency(3, 6);
        g.addDependency(4, 6);
        g.addDependency(6, 7);
        g.addDependency(5, 7);

        g.assertPlan();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testFullCycle()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 2);
        g.addDependency(2, 3);
        g.addDependency(3, 1);

        g.assertPlan();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testEmbeddedCycle()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 2);
        g.addDependency(2, 3);
        g.addDependency(3, 4);
        g.addDependency(4, 2);

        g.assertPlan();
    }

    @Test
    public void testForest()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 2);
        g.addDependency(3, 4);
        g.addDependency(5, 6);
        g.addDependency(7, 8);

        g.assertPlan();
    }

    @Test
    public void testMultiInput()
    {
        GraphCreator g = new GraphCreator();

        g.addDependency(1, 4);
        g.addDependency(2, 4);
        g.addDependency(4, 5);
        g.addDependency(5, 6);
        g.addDependency(6, 7);
        g.addDependency(3, 7);
        g.addDependency(7, 8);

        g.assertPlan();

    }
}
