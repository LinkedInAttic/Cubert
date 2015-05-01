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

package com.linkedin.cubert.functions.builtin;

/**
 * Supported built-in function.
 * 
 * @author Maneesh Varshney
 * 
 */
public enum FunctionType
{
    // Arithmetic functions
    ADD,
    MINUS,
    TIMES,
    DIVIDE,
    MOD,
    LSHIFT,
    RSHIFT,

    // Boolean functions
    NE,
    EQ,
    LT,
    LE,
    GT,
    GE,
    NOT,
    AND,
    OR,
    IS_NULL,
    IS_NOT_NULL,
    IN,
    IsDistinct,

    // type cast functions
    CASTTOLONG,
    CASTTOINT,
    CASTTODOUBLE,
    CASTTOFLOAT,
    CASTTOSTRING,

    // if-else functions
    NVL,
    CASE,

    // String functions
    MATCHES,
    CONCAT,

    // bag functions
    SIZEOF,

    // Serialize to bytearray
    TOBYTEARRAY,
    // zero-argument functions
    UNIQUEID,
    SEQNO;
}
