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

package com.linkedin.cubert.operator.cube;

/**
 * Various types of built-in aggregators.
 * 
 * The aggregator objects can be created using the {@link ValueAggregatorFactory} class.
 * The aggregator objects implement the {@link ValueAggregator} interface.
 * 
 * @author Maneesh Varshney
 * 
 */
public enum ValueAggregationType
{
    SUM, COUNT, MIN, MAX, COUNT_TO_ONE;
}
