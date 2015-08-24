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

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.builtin.Typecast;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Maneesh Varshney
 */
public class TestFunctions {

  @Test
  public void testTypecast()
      throws PreconditionException, ExecException {
    DataType inputType = DataType.BOOLEAN;
    DataType outputType = DataType.STRING;

    BlockSchema inputSchema = new BlockSchema(inputType.toString() + " col");

    Typecast function = new Typecast(outputType);
    function.outputSchema(inputSchema);

    Tuple tuple = TupleFactory.getInstance().newTuple(1);

    tuple.set(0, null);
    Assert.assertEquals(null, function.eval(tuple));

    tuple.set(0, true);
    Assert.assertEquals("true", function.eval(tuple));


    tuple.set(0, false);
    Assert.assertEquals("false", function.eval(tuple));


  }

}
