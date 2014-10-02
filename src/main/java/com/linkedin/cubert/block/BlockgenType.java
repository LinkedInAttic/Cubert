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

package com.linkedin.cubert.block;

/**
 * Specifies how the tuples are accumulated within a block.
 * <ul>
 * <li>BY_ROW creates a block with a fixed number of rows</li>
 * 
 * <li>BY_PARTITION_KEY creates a block with a fixed number of partition keys</li>
 * 
 * <li>BY_SIZE creates a block of a maximum size</li>
 * 
 * <li>BY_INDEX creates a block based on index of partitioning keys from some other
 * relation</li>
 * 
 * <li>USER_DEFINED creates a block based on user-provided cost function.
 * </ul>
 * 
 * @author Maneesh Varshney
 * 
 */
public enum BlockgenType
{
    BY_ROW, BY_PARTITION_KEY, BY_SIZE, BY_INDEX, USER_DEFINED, BY_BLOCK_ID
}
