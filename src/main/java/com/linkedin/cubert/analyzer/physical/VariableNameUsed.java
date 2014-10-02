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

import static com.linkedin.cubert.utils.JsonUtils.asArray;
import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.JsonNode;

/**
 * Plan analyzer that retrives the variable names used in the script.
 * 
 * @author Maneesh Varshney
 * 
 */
public class VariableNameUsed extends PhysicalPlanVisitor
{
    private final Set<String> usedNames = new HashSet<String>();

    public Set<String> getUsedNames()
    {
        return usedNames;
    }

    @Override
    public void visitInput(JsonNode json)
    {
        usedNames.add(getText(json, "name"));
    }

    @Override
    public void visitOperator(JsonNode json, boolean isMapper)
    {
        if (json.has("input"))
        {
            String[] inputs = asArray(json, "input");
            for (String input : inputs)
                usedNames.add(input);
        }

        usedNames.add(getText(json, "output"));
    }

    @Override
    public void visitShuffle(JsonNode json)
    {
        usedNames.add(getText(json, "name"));
    }

    @Override
    public void visitOutput(JsonNode json)
    {
        usedNames.add(getText(json, "name"));
    }

    @Override
    public void visitCachedIndex(JsonNode json)
    {
        usedNames.add(getText(json, "name"));
    }

}
