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

package com.linkedin.cubert.operator;

public enum PreconditionExceptionType
{
    UNDEFINED_POSTCONDITION,
    INPUT_BLOCK_NOT_FOUND,
    DICTIONARY_NOT_FOUND,
    INVALID_SORT_KEYS,
    INVALID_PARTITION_KEYS,
    COLUMN_NOT_PRESENT,
    INVALID_SCHEMA,
    CLASS_NOT_FOUND,
    INVALID_GROUPBY,
    INVALID_DIMENSION_TYPE,
    INVALID_CONFIG,
    MISC_ERROR;
}
