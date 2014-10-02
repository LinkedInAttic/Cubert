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

package com.linkedin.cubert.analyzer.physical;

import org.codehaus.jackson.JsonNode;

/**
 * Visitor for Cubert physical Map-Reduce plan.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PhysicalPlanVisitor
{
    public void enterProgram(JsonNode json)
    {

    }

    public void enterJob(JsonNode json)
    {

    }

    public void visitMap(JsonNode json)
    {

    }

    public void visitInput(JsonNode json)
    {

    }

    public void visitOperator(JsonNode json, boolean isMapper)
    {

    }

    public void visitShuffle(JsonNode json)
    {

    }

    public void visitOutput(JsonNode json)
    {

    }

    public void visitCachedIndex(JsonNode json)
    {

    }

    public void visitCachedFile(String file)
    {

    }

    public void exitJob(JsonNode json)
    {

    }

    public void exitProgram(JsonNode json)
    {

    }
}
