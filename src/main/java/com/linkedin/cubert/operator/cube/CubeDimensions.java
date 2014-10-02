/* (c) 2014 LinkedIn Corp. All rights rimport java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.rcf.block.BlockSchema;
import com.linkedin.rcf.block.DataType;
import com.linkedin.rcf.utils.CodeDictionary;
import com.linkedin.rcf.utils.JsonUtils;
uted on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.operator.cube;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.utils.CodeDictionary;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Manages the dimensions for the CUBE operator.
 * <p>
 * The primary capabilities provided by this class are:
 * <ul>
 * <li>Extracting the dimensions from the input tuple (via the {@link extractDimensionKey}
 * method)</li>
 * 
 * <li>Enumerating the "ancestors" for the dimension keys (via the {@link ancestors}
 * method)</li>
 * 
 * <li>Writing back the dimensions into a tuple (via the {@link outputKey} method).</li>
 * </ul>
 * 
 * @author Maneesh Varshney
 * 
 */
public class CubeDimensions
{
    /*
     * Meta data for dimensions (index in input schema, data type, and offset in the
     * dimension key)
     */
    // the index of dimension columns in the input tuple
    private final int[] inputIndex;

    // the index of the dimension columns in the output tuple
    private final int[] outputIndex;

    // the data type of dimension columns
    private final DataType[] dimensionTypes;

    // the offset of the dimension columns in the int[] array within the {@link
    // DimensionKey}
    private final int[] dimensionOffsets;

    /* information regarding ancestors */
    // total number of ancestors
    private final int numAncestors;

    // is this a full cube
    private final boolean isFullCube;

    // For an ancestor, certain dimensions have "ALL" semantics. In the DimensionKey these
    // dimensions store 0 as their values. The following two arrays provides a fast way to
    // zero out these ALL dimensions

    // For each ancestor, how many ints within the DimensionKey have to be zeroed out (the
    // length of this array is equal to the number of ancestors)
    private final int[] zeroedDimArrayLength;

    // For each ancestor, the actual index within the DimensionKey that have to be zeroed
    // out.
    private final byte[][] zeroedDimIndex;

    /* singleton copies of dimension key and ancestors */
    private final DimensionKey key;
    private final DimensionKey[] ancestors;

    // The last int in the DimensionKey stores the nullBitVector. This variable refers to
    // the index to this int within the array
    private final int nullBitVectorIndex;

    // dictionary for string dimensions
    private final CodeDictionary[] dictionaries;

    public CubeDimensions(BlockSchema inputSchema,
                          BlockSchema outputSchema,
                          String[] dimensions,
                          JsonNode groupingSetsJson)
    {
        // create the arrays
        inputIndex = new int[dimensions.length];
        outputIndex = new int[dimensions.length];
        dimensionTypes = new DataType[dimensions.length];
        dimensionOffsets = new int[dimensions.length];
        dictionaries = new CodeDictionary[dimensions.length];

        // intialize the above arrays
        int idx = 0;
        int offset = 0;
        for (String dim : dimensions)
        {
            inputIndex[idx] = inputSchema.getIndex(dim);
            outputIndex[idx] = outputSchema.getIndex(dim);
            dimensionTypes[idx] = inputSchema.getType(inputIndex[idx]);
            dimensionOffsets[idx] = offset;
            offset++;
            // pad one more int if the data type is long ("encoded" as 2 ints)
            if (dimensionTypes[idx] == DataType.LONG)
                offset++;

            // create dictionary if the dimension is string
            if (dimensionTypes[idx] == DataType.STRING)
                dictionaries[idx] = new CodeDictionary();

            idx++;
        }

        // the "last" int in the dimension key is used to store the null bit vector
        nullBitVectorIndex = offset;

        // create the dimension key
        key = new DimensionKey(nullBitVectorIndex);
        key.getArray()[nullBitVectorIndex] = 0;

        // determine if this is a full cube
        isFullCube =
                (groupingSetsJson == null) || (groupingSetsJson.isNull())
                        || groupingSetsJson.size() == 0;

        // determine the number of ancestors
        if (isFullCube)
            numAncestors = (int) Math.pow(2, dimensions.length);
        else
            numAncestors = groupingSetsJson.size();

        // allocate the ancestors
        ancestors = new DimensionKey[numAncestors];
        for (int i = 0; i < numAncestors; i++)
            ancestors[i] = new DimensionKey(nullBitVectorIndex);

        // pre-assign null bit vector for the ancestors
        assignNullBitVector(dimensions, groupingSetsJson);

        // assign zeroedDimIndex for the ancestors
        zeroedDimArrayLength = new int[numAncestors];
        zeroedDimIndex = new byte[numAncestors][64];
        assignZeroedDimensions(dimensions);
    }

    public int getDimensionKeyLength()
    {
        return nullBitVectorIndex + 1;
    }

    private void assignNullBitVector(String[] dimensions, JsonNode groupingSetsJson)
    {
        // assign null bit vector for the ancestors
        if (isFullCube)
        {
            for (int i = 0; i < numAncestors; i++)
            {
                int bitmap = i;
                int nullBitVector = 0;
                for (int j = 0; j < dimensions.length; j++)
                {
                    boolean lastBitZero = (bitmap % 2 == 0);
                    bitmap >>= 1;

                    if (lastBitZero)
                        nullBitVector = nullBitVector | (1 << j);
                }
                ancestors[i].getArray()[nullBitVectorIndex] = nullBitVector;
            }
        }
        else
        {
            String[] groupingSetsInput = JsonUtils.asArray(groupingSetsJson);

            Map<String, Integer> dimensionPosition = new HashMap<String, Integer>();
            for (int i = 0; i < dimensions.length; i++)
                dimensionPosition.put(dimensions[i], i);

            for (int i = 0; i < groupingSetsInput.length; i++)
            {
                String[] fields = groupingSetsInput[i].split(",");
                int nullBitVector = -1;

                for (int j = 0; j < fields.length; j++)
                {
                    nullBitVector =
                            nullBitVector & ~(1 << dimensionPosition.get(fields[j]));
                }
                ancestors[i].getArray()[nullBitVectorIndex] = nullBitVector;
            }
        }

    }

    private void assignZeroedDimensions(String[] dimensions)
    {

        for (int i = 0; i < numAncestors; i++)
        {
            int nullBitVector = ancestors[i].getArray()[nullBitVectorIndex];
            int idx = 0;
            for (int j = 0; j < dimensions.length; j++)
            {
                boolean isDimZeroed = (nullBitVector & (1 << j)) != 0;
                if (isDimZeroed)
                {
                    zeroedDimIndex[i][idx++] = (byte) dimensionOffsets[j];
                    if (dimensionTypes[j] == DataType.LONG)
                        zeroedDimIndex[i][idx++] = (byte) (dimensionOffsets[j] + 1);
                }
            }
            zeroedDimArrayLength[i] = idx;
        }
    }

    public DimensionKey extractDimensionKey(Tuple tuple) throws ExecException
    {
        int[] array = key.getArray();
        for (int i = 0; i < inputIndex.length; i++)
        {
            Object dim = tuple.get(inputIndex[i]);
            if (dim == null)
                throw new RuntimeException("Dimension is null for tuple " + tuple);

            switch (dimensionTypes[i])
            {
            case INT:
                array[dimensionOffsets[i]] = ((Number) dim).intValue();
                break;
            case LONG:
                long val = ((Number) dim).longValue();
                array[dimensionOffsets[i]] = (int) (val >> 32); // upper 32 bits
                array[dimensionOffsets[i] + 1] = (int) val; // lower 32 bits
                break;
            case STRING:
                CodeDictionary dict = dictionaries[i];
                int code = dict.getCodeForKey((String) dim);
                if (code == -1)
                    code = dict.addKey((String) dim);
                array[dimensionOffsets[i]] = code;
                break;
            default:
                throw new RuntimeException("Type of dimension is not INT, LONG or STRING for tuple "
                        + tuple + " at col " + i);
            }
        }

        return key;
    }

    public DimensionKey[] ancestors(DimensionKey key)
    {
        for (int i = 0; i < numAncestors; i++)
        {
            int[] array = ancestors[i].getArray();
            // copy the int[] from the original key
            System.arraycopy(key.getArray(), 0, array, 0, nullBitVectorIndex);

            // zero out the dimensions that have null bit set
            for (int j = 0; j < zeroedDimArrayLength[i]; j++)
                array[zeroedDimIndex[i][j]] = 0;
        }

        return ancestors;
    }

    public DimensionKey[] ancestors(Tuple tuple) throws ExecException
    {
        return ancestors(extractDimensionKey(tuple));
    }

    public void outputKey(DimensionKey key, Tuple outputTuple) throws ExecException
    {
        int[] array = key.getArray();
        int nullBitVector = array[nullBitVectorIndex];

        for (int dim = 0; dim < outputIndex.length; dim++)
        {
            int dimIndex = outputIndex[dim];
            boolean isNull = (nullBitVector & (1 << dim)) != 0;

            if (isNull)
            {
                outputTuple.set(dimIndex, null);
            }
            else
            {
                switch (dimensionTypes[dim])
                {
                case INT:
                    outputTuple.set(dimIndex, array[dimensionOffsets[dim]]);
                    break;
                case LONG:
                    long val = (long) (array[dimensionOffsets[dim]]);
                    val = (val << 32) | (array[dimensionOffsets[dim] + 1] & 0xFFFFFFFFL);
                    outputTuple.set(dimIndex, val);
                    break;
                case STRING:
                    CodeDictionary dict = dictionaries[dim];
                    String str = dict.getValueForCode(array[dimensionOffsets[dim]]);
                    outputTuple.set(dimIndex, str);
                    break;
                default:
                    throw new RuntimeException("Type of dimension is not INT, LONG or STRING for tuple ");
                }
            }
        }
    }
}
