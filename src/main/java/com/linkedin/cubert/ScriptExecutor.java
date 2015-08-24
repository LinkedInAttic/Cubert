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

package com.linkedin.cubert;

import com.linkedin.cubert.app.CmrExecutor;
import com.linkedin.cubert.app.ExecutionSpec;
import com.linkedin.cubert.utils.FileSystemUtils;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Compiles and executes cubert scripts.
 *
 * @author Maneesh Varshney
 */
public class ScriptExecutor
{
    private static String CUBERT_PROP_IDENTIFIER = "param.";

    private String azkabanJobName;
    private Properties azkabanProperties;

    public ScriptExecutor(String name, Properties props)
    {
        this.azkabanJobName = name;
        this.azkabanProperties = props;
    }

    /**
     * Called from Azkaban executor to launch job(s)
     *
     * @throws Exception
     */
    public void run()
            throws Exception
    {
        String scriptName = azkabanProperties.getProperty("cubert.script");
        if (scriptName == null)
            throw new IllegalArgumentException("Cubert script name is not provided.");

        String argsStr = azkabanProperties.getProperty("cubert.args");
        if (argsStr != null)
        {
            // TODO: fix this
            if (argsStr.contains("\"") || argsStr.contains("'"))
                throw new IllegalArgumentException(
                        "Oops! The parser does not support quotes in the args. Please use -f <params file> for now.");

            scriptName = scriptName + " " + argsStr;
        }

        String[] args = scriptName.split("\\s+");

        CommandLine cmdLine = getCommandLine(args);

        ExecutionSpec spec = getExecutionSpec(cmdLine);
        if (spec == null)
        {
            return;
        }

        CmrExecutor cmrExecutor = new CmrExecutor();
        cmrExecutor.run(spec, azkabanProperties);
    }

    /**
     * Called from Azkaban executor to terminate job(s)
     */
    public void cancel()
    {
        // TODO: kill launched jobs.
    }

    public static void main(String[] args)
            throws Exception
    {
        CommandLine cmdLine = getCommandLine(args);
        if (cmdLine == null)
            return;

        ExecutionSpec spec = getExecutionSpec(cmdLine);
        if (spec == null)
            return;

        Properties prop = getProperties(cmdLine, null);

        CmrExecutor cmrExecutor = new CmrExecutor();
        cmrExecutor.run(spec, prop);
    }

    private static ExecutionSpec getExecutionSpec(CommandLine cmdLine)
            throws IOException
    {
        String[] remainingArgs = cmdLine.getArgs();

        if (remainingArgs.length == 0)
        {
            System.err.println("Cubert script file not specified");
            return null;
        }

        String program = readFile(new File(remainingArgs[0]));

        ExecutionSpec spec = new ExecutionSpec(program);

        if (cmdLine.hasOption('P'))
        {
            String value = cmdLine.getOptionValue('P');

            FileSystem localFs = FileSystem.getLocal(new Configuration());

            for (String pathStr : value.split(":"))
            {
                List<Path> paths = FileSystemUtils.getPaths(localFs, new Path(pathStr));

                for (Path p : paths)
                    spec.addJar(p);
            }
        }

        if (cmdLine.hasOption("x"))
        {
            for (String job : cmdLine.getOptionValues("x"))
                spec.addJobs(job);
        }

        spec.setParallel(cmdLine.hasOption("parallel"))
            .setDebugMode(cmdLine.hasOption("d"))
            .setProfileMode(cmdLine.hasOption("perf"))
            .setPrintJson(cmdLine.hasOption("j"))
            .setDescribe(cmdLine.hasOption("describe"))
            .setPreprocessOnly(cmdLine.hasOption("s"))
            .setParseOnly(cmdLine.hasOption("p"))
            .setCompileOnly(cmdLine.hasOption("c"));

        return spec;
    }

    /**
     * Properties are collected in the following order--
     * <p/>
     * 1. Azkaban (or other) executor params
     * 2. properties passed through -f argument (for multiple order in CLI order)
     * 3. properties passed as -D arguments directly on CLI
     *
     * @param cmdLine
     * @param executorProps
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    private static Properties getProperties(CommandLine cmdLine, Properties executorProps)
            throws URISyntaxException, IOException
    {
        Properties props = new Properties();

        // 1. Substitute executor params
        if (executorProps != null)
        {
            props.putAll(extractCubertParams(executorProps));
        }

        // 2. -f properties
        String[] propFiles = cmdLine.getOptionValues("f");
        if (propFiles != null && propFiles.length > 0)
        {
            for (String propFile : propFiles)
            {
                URI uri = new URI(propFile);
                boolean isHDFS = (uri.getScheme() != null) && uri.getScheme()
                                                                 .equalsIgnoreCase("hdfs");
                String path = uri.getPath();
                if (isHDFS)
                {
                    props.load(new BufferedReader(new InputStreamReader(FileSystem.get(new JobConf())
                                                                                  .open(new Path(
                                                                                          path)))));
                }
                else
                {
                    props.load(new BufferedReader(new FileReader(path)));
                }
            }
        }

        // 3. -D properties
        if (cmdLine.getOptionProperties("D").size() > 0)
        {
            props.putAll(cmdLine.getOptionProperties("D"));
        }
        return props;
    }

    private static Map<String, String> extractCubertParams(Properties executorProps)
    {
        Map<String, String> cubertParams = new HashMap<String, String>();

        int stripLen = CUBERT_PROP_IDENTIFIER.length();
        String regEx = CUBERT_PROP_IDENTIFIER + "*";

        for (String p : executorProps.stringPropertyNames())
        {
            if (!p.matches(regEx))
                continue;

            String key = p.substring(stripLen, p.length());
            String value = executorProps.getProperty(p);

            cubertParams.put(key, value);
        }

        return cubertParams;
    }

    private static String readFile(File file)
            throws IOException
    {
        InputStream in = new FileInputStream(file);
        BufferedReader breader = new BufferedReader(new InputStreamReader(in));
        StringBuilder strBuilder = new StringBuilder();
        String line;
        while ((line = breader.readLine()) != null)
        {
            strBuilder.append(line);
            strBuilder.append("\n");
        }

        breader.close();

        return strBuilder.toString();
    }

    private static CommandLine getCommandLine(String[] args)
            throws ParseException
    {
        Options options = new Options();

        options.addOption("s",
                          "preprocess",
                          false,
                          "show the script after preprocessing");
        options.addOption("j", "json", false, "show the plan in JSON");
        options.addOption("p", "parse", false, "stop after parsing");
        options.addOption("c", "compile", false, "stop after compilation");
        options.addOption("d", "debug", false, "print debuging information");
        options.addOption("perf", false, "enable performance profiling");
        options.addOption("h", "help", false, "shows this message");
        options.addOption(new Option("describe",
                                     "describe the schemas of output datasets"));

        options.addOption(OptionBuilder.withArgName("file")
                                       .hasArg()
                                       .withDescription("use given parameter file")
                                       .withLongOpt("param_file")
                                       .create("f"));

        options.addOption(OptionBuilder.withArgName("lib path")
                                       .hasArg()
                                       .withDescription(
                                               "classpath to be uploaded to distributed cache")
                                       .withLongOpt("cache_path")
                                       .create("P"));

        options.addOption(OptionBuilder.withArgName("property=value")
                                       .hasArgs(2)
                                       .withValueSeparator()
                                       .withDescription("use value for given property")
                                       .create("D"));

        options.addOption(OptionBuilder.withArgName("job id/name")
                                       .hasArgs()
                                       .withDescription("execute this job only")
                                       .create("x"));

        options.addOption(new Option("parallel", "run independent jobs in parallel"));

        // create the parser
        CommandLineParser parser = new PosixParser();

        // parse the command line arguments
        CommandLine line = parser.parse(options, args);

        if (line.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ScriptExecutor <cubert script file> [options]", options);
            return null;
        }

        return line;
    }
}
