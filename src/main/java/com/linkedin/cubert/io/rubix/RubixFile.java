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

package com.linkedin.cubert.io.rubix;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.print;
import com.linkedin.cubert.utils.ClassCache;

public class RubixFile<K, V>
{
    private final Path path;
    private final Configuration conf;
    private JsonNode metadataJson;
    private Class<K> keyClass;
    private Class<V> valueClass;

    private List<KeyData<K>> keyData = null;

    public static class KeyData<K>
    {
        private final K key;
        private final long offset;
        private long length;
        private long blockId;
        private long numRecords;

        public KeyData(K key, long offset, long length, long numRecs, long blockId)
        {
            this.key = key;
            this.offset = offset;
            this.length = length;
            this.numRecords = numRecs;
            this.blockId = blockId;
        }

        public K getKey()
        {
            return key;
        }

        public long getBlockId()
        {
            return blockId;
        }

        public int getReducerId()
        {
            return (int) (getBlockId() >> 32);
        }

        public long getNumRecords()
        {
            return numRecords;
        }

        public long getOffset()
        {
            return offset;
        }

        public long getLength()
        {
            return length;
        }

        void setLength(long length)
        {
            this.length = length;
        }

        @Override
        public String toString()
        {
            return String.format("[key=%s, offset=%d, length=%d, numRecords=%d, blockId=%d]",
                                 key,
                                 offset,
                                 length,
                                 numRecords,
                                 blockId);
        }
    }

    public RubixFile(Configuration conf, Path path)
    {
        this.conf = conf;
        this.path = path;
    }

    public Class<K> getKeyClass() throws IOException,
            InstantiationException,
            IllegalAccessException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        return keyClass;
    }

    public Class<V> getValueClass() throws IOException,
            InstantiationException,
            IllegalAccessException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        return valueClass;
    }

    public BlockSchema getSchema() throws IOException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        return new BlockSchema(metadataJson.get("schema"));
    }

    public String[] getPartitionKeys() throws IOException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        return JsonUtils.asArray(metadataJson.get("partitionKeys"));
    }

    public String[] getSortKeys() throws IOException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        return JsonUtils.asArray(metadataJson.get("sortKeys"));
    }

    public BlockSerializationType getBlockSerializationType() throws IOException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        if (!metadataJson.has("serializationType"))
            return BlockSerializationType.DEFAULT;

        return BlockSerializationType.valueOf(JsonUtils.getText(metadataJson,
                                                                "serializationType"));
    }

    public String getBlockgenId() throws IOException,
            ClassNotFoundException
    {
        if (keyData == null)
            getKeyData();

        if (!metadataJson.has("BlockgenId"))
            return null;
        return JsonUtils.getText(metadataJson, "BlockgenId");
    }

    @SuppressWarnings("unchecked")
    public List<KeyData<K>> getKeyData() throws IOException,
            ClassNotFoundException
    {
        if (keyData != null)
            return keyData;

        final FileSystem fs = FileSystem.get(conf);
        keyData = new ArrayList<KeyData<K>>();

        final long filesize = fs.getFileStatus(path).getLen();
        FSDataInputStream in = fs.open(path);

        /* The last long in the file is the start position of the trailer section */
        in.seek(filesize - 8);
        long metaDataStartPos = in.readLong();

        in.seek(metaDataStartPos);

        ObjectMapper mapper = new ObjectMapper();
        metadataJson = mapper.readValue(in.readUTF(), JsonNode.class);

        int keySectionSize = in.readInt();

        // load the key section
        byte[] keySection = new byte[keySectionSize];

        in.seek(filesize - keySectionSize - 8);
        in.read(keySection, 0, keySectionSize);
        in.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(keySection);
        DataInput dataInput = new DataInputStream(bis);

        int numberOfBlocks = metadataJson.get("numberOfBlocks").getIntValue();

        // load the key section
        keyClass = (Class<K>) ClassCache.forName(JsonUtils.getText(metadataJson, "keyClass"));
        valueClass =
                (Class<V>) ClassCache.forName(JsonUtils.getText(metadataJson, "valueClass"));

        SerializationFactory serializationFactory = new SerializationFactory(conf);
        Deserializer<K> deserializer = serializationFactory.getDeserializer(keyClass);

        deserializer.open(bis);

        while (bis.available() > 0 && numberOfBlocks > 0)
        {
            K key = deserializer.deserialize(null);

            long offset = dataInput.readLong();
            long blockId = dataInput.readLong();
            long numRecords = dataInput.readLong();

            keyData.add(new KeyData<K>(key, offset, 0, numRecords, blockId));
            numberOfBlocks--;
        }

        // Assign length to each keydata entry
        int numEntries = keyData.size();
        for (int i = 1; i < numEntries; i++)
        {
            KeyData<K> prev = keyData.get(i - 1);
            KeyData<K> current = keyData.get(i);

            prev.setLength(current.getOffset() - prev.getOffset());
        }

        if (numEntries > 0)
        {
            KeyData<K> last = keyData.get(numEntries - 1);
            last.setLength(metaDataStartPos - last.offset);
        }

        return keyData;
    }

    private static void extract(List<RubixFile<Tuple, Object>> rfiles,
                                long blockId, int numBlocks,
                                String output) throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
          Configuration conf = new JobConf();
          File outFile = new File(output);
          if (outFile.exists())
          {
              outFile.delete();
          }
          outFile.createNewFile();
          BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outFile));
          ByteArrayOutputStream keySectionStream = new ByteArrayOutputStream();
          DataOutput keySectionOut = new DataOutputStream(keySectionStream);
          SerializationFactory serializationFactory = new SerializationFactory(conf);
          RubixFile<Tuple,Object> lastrFile = null;
          JsonNode json;
          long totalLength = 0;

          final int BUF_SIZE = 32 * 1024;
          long blockIds[] = new long[numBlocks];
          int foundBlocks = 0;

          for(int i=0;i<numBlocks;i++)
            blockIds[i] = blockId+i;

          for(int i=0;i<numBlocks;i++)
          {
            boolean found = false;
            for (RubixFile<Tuple, Object> rfile : rfiles)
            {
              print.f("Checking %s", rfile.path.toString());
              List<KeyData<Tuple>> keyDataList = rfile.getKeyData();
              for (KeyData<Tuple> keyData : keyDataList)
              {
                  if (keyData.getBlockId() == blockIds[i])
                  {
                    long offset = keyData.getOffset();
                    long length = keyData.getLength();
                    Tuple key = keyData.getKey();
                    print.f("Extracting block %d (off=%d len=%d) from %s",
                        keyData.getBlockId(),
                        offset,
                        length,
                        rfile.path.toString());

                 // copy the data
                    if (length > 0)
                    {
                        FileSystem fs = FileSystem.get(conf);
                        FSDataInputStream in = fs.open(rfile.path);
                        in.seek(offset);

                        byte[] data = new byte[BUF_SIZE];
                        long toRead = length;
                        while (toRead > 0)
                        {
                            int thisRead = toRead > BUF_SIZE ? BUF_SIZE : (int) toRead;
                            in.readFully(data, 0, thisRead);
                            bos.write(data, 0, thisRead);
                            toRead -= thisRead;
                            System.out.print(".");
                        }
                        System.out.println();
                    }
                    // copy the key section
                    Serializer<Tuple> keySerializer =
                        serializationFactory.getSerializer(rfile.getKeyClass());
                    keySerializer.open(keySectionStream);

                    keySerializer.serialize(key);
                    keySectionOut.writeLong(totalLength); // position
                    keySectionOut.writeLong(keyData.getBlockId());
                    keySectionOut.writeLong(keyData.getNumRecords());
                    foundBlocks++;
                    totalLength += length;
                    lastrFile = rfile;

                    found = true;
                    break;

                  }
              }
              if(found){
                break;
              }
          }
          if(!found)
            System.err.println("Cannot locate block with id " + blockIds[i]);
        }
        byte[] trailerBytes = keySectionStream.toByteArray();

        json = JsonUtils.cloneNode(lastrFile.metadataJson);
        ((ObjectNode) json).put("numberOfBlocks", foundBlocks);

        DataOutput out = new DataOutputStream(bos);
        out.writeUTF(json.toString());
        out.writeInt(trailerBytes.length);
        out.write(trailerBytes);
        out.writeLong(totalLength); // trailer start offset
        bos.close();
    }

    private static void dumpAvro(List<RubixFile<Tuple, Object>> rfiles, String output) throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            InterruptedException
    {
        // Configuration conf = new JobConf();
        // File outDir = new File(output);
        // if (outDir.exists())
        // outDir.delete();
        // outDir.mkdirs();
        //
        // RubixFile<Tuple, Object> firstFile = rfiles.get(0);
        // BlockSchema schema = firstFile.getSchema();
        // Schema avroSchema = AvroUtils.convertFromBlockSchema("recordName", schema);
        // AvroBlockWriter avroBlockWriter = new AvroBlockWriter();
        // Record record = avroBlockWriter.createRecord(avroSchema);
        //
        // for (RubixFile<Tuple, Object> rfile : rfiles)
        // {
        // Path inPath = rfile.path;
        //
        // List<KeyData<Tuple>> keyDataList = rfile.getKeyData();
        // File outPath = new File(outDir, inPath.getName() + ".avro");
        // outPath.createNewFile();
        //
        // GenericDatumWriter<GenericRecord> datumWriter =
        // new GenericDatumWriter<GenericRecord>(avroSchema);
        // DataFileWriter<GenericRecord> dataFileWriter =
        // new DataFileWriter<GenericRecord>(datumWriter);
        // dataFileWriter.create(avroSchema, outPath);
        //
        // for (KeyData<Tuple> keyData : keyDataList)
        // {
        //
        // RubixInputSplit<Tuple, Object> split =
        // new RubixInputSplit<Tuple, Object>(conf,
        // inPath,
        // keyData.getKey(),
        // keyData.getOffset(),
        // keyData.getLength(),
        // keyData.getBlockId(),
        // keyData.getNumRecords(),
        // rfile.getKeyClass(),
        // rfile.getValueClass(),
        // rfile.getSchema(),
        // rfile.getBlockSerializationType());
        // RubixRecordReader<Tuple, Object> recordReader =
        // new RubixRecordReader<Tuple, Object>();
        // recordReader.initialize(split, conf);
        //
        // while (recordReader.nextKeyValue())
        // {
        // Tuple tuple = (Tuple) recordReader.getCurrentValue();
        // for (int i = 0; i < schema.getNumColumns(); i++)
        // {
        // avroBlockWriter.writeField(record,
        // i,
        // tuple.get(i),
        // avroSchema.getFields().get(i).schema());
        // }
        // dataFileWriter.append(record);
        // }
        // }
        // dataFileWriter.close();
        // System.out.println("Written " + outPath);
        // }
    }

    private static void dumpText(List<RubixFile<Tuple, Object>> rfiles,
                                 String output,
                                 int numRows) throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        Configuration conf = new JobConf();
        int totalBlocks = 0;

        for (RubixFile<Tuple, Object> rfile : rfiles)
        {
            Path path = rfile.path;
            List<KeyData<Tuple>> keyDataList = rfile.getKeyData();

            print.f("--- %s", path.toString());
            print.f("Schema: %s", rfile.getSchema().toString());
            print.f("PartitionKeys: %s", Arrays.toString(rfile.getPartitionKeys()));
            print.f("SortKeys %s", Arrays.toString(rfile.getSortKeys()));
            print.f("Block Serialization Type: %s", rfile.getBlockSerializationType());
            print.f("Number of blocks: %d", keyDataList.size());

            totalBlocks += keyDataList.size();

            int cumrows = 0;

            for (KeyData<Tuple> keyData : keyDataList)
            {
                print.f("Block %s. BlockId: %d (Reducer: %d Index:%d)",
                        keyData,
                        keyData.blockId,
                        (keyData.getBlockId() >> 32),
                        (keyData.getBlockId() & (((long) 1 << 32) - 1)));

                if (numRows > 0)
                {
                    RubixInputSplit<Tuple, Object> split =
                            new RubixInputSplit<Tuple, Object>(conf,
                                                               path,
                                                               keyData.getKey(),
                                                               keyData.getOffset(),
                                                               keyData.getLength(),
                                                               keyData.getBlockId(),
                                                               keyData.getNumRecords(),
                                                               rfile.getKeyClass(),
                                                               rfile.getValueClass(),
                                                               rfile.getSchema(),
                                                               rfile.getBlockSerializationType());

                    RubixRecordReader<Tuple, Object> recordReader =
                            new RubixRecordReader<Tuple, Object>();
                    recordReader.initialize(split, conf);
                    int rows = 0;

                    while (recordReader.nextKeyValue())
                    {
                        rows++;
                        if (rows < numRows)
                        {
                            System.out.println("\t" + recordReader.getCurrentValue());
                        }
                        else
                        {
                            break;
                        }
                    }

                    cumrows += keyData.getNumRecords();
                    System.out.println(String.format("\tRows=%d Cummulative=%d",
                                                     keyData.getNumRecords(),
                                                     cumrows));
                }
            }
        }

        print.f("Total Blocks: %d", totalBlocks);
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException,
            ParseException,
            InstantiationException,
            IllegalAccessException
    {
        final int VERBOSE_NUM_ROWS = 4;

        Options options = new Options();

        options.addOption("h", "help", false, "shows this message");
        options.addOption("v",
                          "verbose",
                          false,
                          "print summary and first few rows of each block");
        options.addOption("m", "metadata", false, "show the metadata");
        options.addOption("d",
                          "dump",
                          false,
                          "dump the contents of the rubix file. Use -f for specifying format, and -o for specifying "
                          + "output location");
        options.addOption("f",
                          "format",
                          true,
                          "the data format for dumping data (AVRO or TEXT). Default: TEXT");
        options.addOption("e",
                          "extract",
                          true,
                          "Extract one or more rubix blocks starting from the given blockId. Use -e blockId,numBlocks "
                          + "for specifying the blocks to be extracted. Use -o for specifying output location");
        options.addOption("o", true, "Store the output at the specified location");

        CommandLineParser parser = new BasicParser();

        // parse the command line arguments
        CommandLine line = parser.parse(options, args);

        // show the help message
        if (line.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("RubixFile <rubix file or dir> [options]\nIf no options are provided, print a summary of the blocks.",
                                options);
            return;
        }

        // validate provided options
        if (line.hasOption("d") && line.hasOption("e"))
        {
            System.err.println("Cannot dump (-d) and extract (-e) at the same time!");
            return;
        }

        // obtain the list of rubix files
        String[] files = line.getArgs();
        if (files == null || files.length == 0)
        {
            System.err.println("Rubix file not specified");
            return;
        }

        Configuration conf = new JobConf();
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(files[0]);
        FileStatus[] allFiles;

        FileStatus status = fs.getFileStatus(path);
        if (status.isDir())
        {
            allFiles = fs.listStatus(path, new PathFilter()
            {
                @Override
                public boolean accept(Path path)
                {
                    return path.toString().contains(RubixConstants.RUBIX_EXTENSION);
                }

            });
        }
        else
        {
            allFiles = new FileStatus[] { status };
        }

        // walk over all files and extract the trailer section
        List<RubixFile<Tuple, Object>> rfiles = new ArrayList<RubixFile<Tuple, Object>>();

        for (FileStatus s : allFiles)
        {
            Path p = s.getPath();

            RubixFile<Tuple, Object> rfile = new RubixFile<Tuple, Object>(conf, p);

            // if printing meta data information.. exit after first file (since all files
            // have the same meta data)
            if (line.hasOption("m"))
            {
                rfile.getKeyData();

                System.out.println(new ObjectMapper().writer()
                                                     .writeValueAsString(rfile.metadataJson));
                break;
            }

            rfiles.add(rfile);
        }

        // dump the data
        if (line.hasOption("d"))
        {
            String format = line.getOptionValue("f");
            if (format == null)
                format = "TEXT";

            format = format.trim().toUpperCase();

            if (format.equals("AVRO"))
            {
                // dumpAvro(rfiles, line.getOptionValue("o"));
                throw new UnsupportedOperationException("Dumping to avro is not currently supporting. Please write a Cubert (map-only) script to store data in avro format");
            }
            else if (format.equals("TEXT"))
            {
                if (line.hasOption("o"))
                {
                    System.err.println("Dumping TEXT format data *into a file* is not currently supported");
                    return;
                }
                dumpText(rfiles, line.getOptionValue("o"), Integer.MAX_VALUE);
            }
            else
            {
                System.err.println("Invalid format [" + format
                        + "] for dumping. Please use AVRO or TEXT");
                return;
            }
        }
        // extract arguments: -e blockId,numBlocks(contiguous) -o ouputLocation
        else if (line.hasOption("e"))
        {
            String extractArguments = line.getOptionValue("e");
            String outputLocation;
            if(line.hasOption("o"))
            {
              outputLocation = line.getOptionValue("o");
            }
            else
            {
              System.err.println("Need to specify the location to store the output");
              return;
            }
            long blockId;
            int numBlocks = 1;
            if(extractArguments.contains(","))
            {
              String[] splitExtractArgs = extractArguments.split(",");
              blockId = Long.parseLong(splitExtractArgs[0]);
              numBlocks = Integer.parseInt(splitExtractArgs[1]);
            }
            else
            {
              blockId = Long.parseLong(extractArguments);
            }

            extract(rfiles, blockId, numBlocks, outputLocation);
        }
        else
        // print summary
        {
            dumpText(rfiles, null, line.hasOption("v") ? VERBOSE_NUM_ROWS : 0);
        }
    }
}
