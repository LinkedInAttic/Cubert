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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.analyzer.physical.BlockgenLineageAnalyzer;
import com.linkedin.cubert.analyzer.physical.CachedFileAnalyzer;
import com.linkedin.cubert.analyzer.physical.DependencyAnalyzer;
import com.linkedin.cubert.analyzer.physical.DescribePlan;
import com.linkedin.cubert.analyzer.physical.DictionaryAnalyzer;
import com.linkedin.cubert.analyzer.physical.OverwriteAnalyzer;
import com.linkedin.cubert.analyzer.physical.PhysicalPlanWalker;
import com.linkedin.cubert.analyzer.physical.PlanRewriteException;
import com.linkedin.cubert.analyzer.physical.PlanRewriter;
import com.linkedin.cubert.analyzer.physical.SemanticAnalyzer;
import com.linkedin.cubert.analyzer.physical.ShuffleRewriter;
import com.linkedin.cubert.analyzer.physical.VariableNameUsed;
import com.linkedin.cubert.plan.physical.ExecutorService;
import com.linkedin.cubert.plan.physical.PhysicalParser;
import com.linkedin.cubert.utils.ExecutionConfig;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Compiles and executes cubert scripts.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ScriptExecutor
{
    private String program;
    private JsonNode physicalPlan;
    private boolean debugMode = false;

    public ScriptExecutor(String program)
    {
        this.program = program;
    }

    public ScriptExecutor(File filename) throws IOException
    {
        program = readFile(filename);
    }

    public String getProgram()
    {
        return program;
    }

    private String replaceVariables(String program, Properties props)
    {
        List<Object> keys = new ArrayList<Object>(props.keySet());
        Collections.sort(keys, new Comparator<Object>()
        {
            @Override
            public int compare(Object o1, Object o2)
            {
                String s1 = (String) o1;
                String s2 = (String) o2;
                return s2.length() - s1.length();
            }
        });

        for (Object key : keys)
        {
            program =
                    program.replaceAll("\\$" + key,
                                       Matcher.quoteReplacement(((String) props.get(key))));
        }
        return program;
    }

    public void preprocess(Properties props) throws InstantiationException,
            IllegalAccessException,
            ClassNotFoundException,
            IOException,
            InterruptedException,
            ParseException,
            ScriptException
    {
        if (props == null)
            props = new Properties();

        // get the list of macro variables in the script
        Matcher matcher = Pattern.compile("\\$[a-zA-Z_][a-zA-Z0-9_]*").matcher(program);
        Set<String> variables = new HashSet<String>();
        while (matcher.find())
        {
            String key = matcher.group(0);
            key = key.substring(1, key.length()); // remove the leading $
            variables.add(key);
        }

        // check if the script has javascript
        matcher =
                Pattern.compile("<javascript>(.*?)</javascript>", Pattern.DOTALL)
                       .matcher(program);

        boolean scriptFound = false;
        StringBuilder scriptBuilder = new StringBuilder();
        StringBuffer sb = new StringBuffer();

        while (matcher.find())
        {
            scriptFound = true;
            scriptBuilder.append(matcher.group(1));
            matcher.appendReplacement(sb, "");
        }
        matcher.appendTail(sb);

        // if it does, then execute the javascript
        if (scriptFound)
        {
            program = sb.toString();
            String javascript = scriptBuilder.toString();

            // get the java script engine
            ScriptEngine engine =
                    (new ScriptEngineManager()).getEngineByName("JavaScript");

            // introduce the base properties within the javascript engine
            for (Object key : props.keySet())
            {
                String escapedKey = ((String) key).replaceAll("[^A-Za-z0-9]", "");
                String cmd =
                        String.format("var %s = \"%s\";",
                                      escapedKey,
                                      props.getProperty((String) key));
                engine.eval(cmd);
            }

            // execute the java script
            engine.eval(javascript);

            // go over all macros variables and see if they are defined in the java script
            Properties scriptProps = new Properties();
            for (String variable : variables)
            {
                // see if it is defined in the javascript
                String snippet = String.format("typeof %s !== 'undefined'", variable);
                if ((Boolean) engine.eval(snippet))
                {
                    String value = engine.eval(variable).toString();
                    scriptProps.put(variable, value);
                }
            }

            // replace the variables found in the script
            if (scriptProps.size() > 0)
                program = replaceVariables(program, scriptProps);
        }

        // The macro variables that are not already defined in the props, may be defined
        // as environment variables
        for (String variable : variables)
        {
            if (!props.containsKey(variable))
            {
                String value = System.getenv(variable);
                if (value != null)
                    props.put(variable, value);
            }
        }

        // Substitute the base variable
        if (props != null && props.size() > 0)
            program = replaceVariables(program, props);

        // Substitute backticks
        substituteBackticks();
    }

    private void substituteBackticks() throws IOException,
            InterruptedException
    {
        Pattern pattern = Pattern.compile("`([^`]+)`");
        Matcher m = pattern.matcher(program);
        StringBuffer sb = new StringBuffer();
        while (m.find())
        {
            final String cmdString = m.group(1);
            String[] cmdArgs = new String[3];
            cmdArgs[0] = "bash";
            cmdArgs[1] = "-c";
            cmdArgs[2] = "exec " + cmdString;
            final Process p = Runtime.getRuntime().exec(cmdArgs);
            p.waitFor();

            final InputStream inputStream = p.getInputStream();
            byte[] buf = new byte[inputStream.available()];
            inputStream.read(buf); // TODO: read fully

            String repl = new String(buf).trim().replace("$", "\\$");
            m.appendReplacement(sb, repl);
        }
        m.appendTail(sb);
        program = sb.toString();
    }

    public void compile() throws IOException
    {
        physicalPlan = PhysicalParser.parseProgram(getProgram());
    }

    public void rewrite() throws IOException,
            InstantiationException,
            IllegalAccessException
    {
        VariableNameUsed nameUsedVisitor = new VariableNameUsed();
        new PhysicalPlanWalker(physicalPlan, nameUsedVisitor).walk();
        Set<String> namesUsed = nameUsedVisitor.getUsedNames();

        List<Class<? extends PlanRewriter>> rewriters =
                new ArrayList<Class<? extends PlanRewriter>>();

        rewriters.add(ShuffleRewriter.class);
        rewriters.add(CachedFileAnalyzer.class);
        rewriters.add(DependencyAnalyzer.class);
        rewriters.add(OverwriteAnalyzer.class);
        rewriters.add(DictionaryAnalyzer.class);
        rewriters.add(BlockgenLineageAnalyzer.class);
        rewriters.add(SemanticAnalyzer.class);

        for (Class<? extends PlanRewriter> rewriterClass : rewriters)
        {

            physicalPlan =
                    rewriterClass.newInstance().rewrite(physicalPlan,
                                                        namesUsed,
                                                        debugMode,
                                                        false);

            if (physicalPlan == null)
                return;
        }

    }

    public void execute(int jobId) throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        ExecutorService executorService = new ExecutorService(physicalPlan);
        executorService.execute(jobId);
    }

    public void execute() throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        ExecutorService executorService = new ExecutorService(physicalPlan);
        executorService.execute();
    }

    @SuppressWarnings("deprecation")
    public void printPhysical() throws JsonGenerationException,
            JsonMappingException,
            IOException
    {
        System.out.println(new ObjectMapper().defaultPrettyPrintingWriter()
                                             .writeValueAsString(physicalPlan));
    }

    private void setDebugMode(boolean hasOption)
    {
        this.debugMode = hasOption;
    }

    private void setProfileMode(boolean hasOption)
    {
        if (physicalPlan != null)
            ((ObjectNode) physicalPlan).put("profileMode", hasOption);
    }

    public void setParallelExecution(String value)
    {
        JsonNode hadoopConf = this.physicalPlan.get("hadoopConf");
        ((ObjectNode) hadoopConf).put("parallel", value);
        ((ObjectNode) this.physicalPlan).put("hadoopConf", hadoopConf);
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            ScriptException,
            ParseException
    {
        Options options = new Options();

        options.addOption("s", "preprocess", false, "show the script after preprocessing");
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

        if (line.hasOption("parallel"))
        {
            ExecutionConfig.getInstance().setParallelExec(true);
        }
        else
        {
            ExecutionConfig.getInstance().setParallelExec(false);
        }

        if (line.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ScriptExecutor <cubert script file> [options]", options);
            return;
        }
        String[] remainingArgs = line.getArgs();
        if (remainingArgs == null || remainingArgs.length == 0)
        {
            System.err.println("Cubert script file not specified");
            return;
        }

        ScriptExecutor exec = new ScriptExecutor(new File(remainingArgs[0]));
        exec.setDebugMode(line.hasOption("d"));

        // Step 1: Preprocess the file
        Properties props = new Properties();
        String[] propFiles = line.getOptionValues("f");
        if (propFiles != null && propFiles.length > 0)
        {
            for (String propFile : propFiles)
                props.load(new BufferedReader(new FileReader(propFile)));
        }

        if (line.getOptionProperties("D").size() > 0)
        {
            props.putAll(line.getOptionProperties("D"));
        }

        exec.preprocess(props);

        // command line option: if -s is set, show the code and exit
        if (line.hasOption("s"))
        {
            System.out.println(exec.getProgram());
            return;
        }

        // Step 2: Translate to physical json plan
        try
        {
            // Step 2: Translate to physical json plan
            exec.compile();

            if (line.hasOption("p"))
                return;

            // Step 3: rewrite plan,
            exec.rewrite();
            exec.setProfileMode(line.hasOption("perf"));

            if (line.hasOption("c"))
                return;
        }
        catch (PlanRewriteException e)
        {
            System.err.println(e.getMessage());
            if (line.hasOption("d"))
                e.printStackTrace(System.err);

            System.err.println("\nCannot compile cubert script. Exiting.");
            return;
        }
        catch (Exception e)
        {

            System.err.println("\nCannot compile cubert script. Exiting.");
            e.printStackTrace(System.err);
            return;
        }
        finally
        {
            if (line.hasOption("j"))
                exec.printPhysical();
        }

        if (line.hasOption("describe"))
        {
            new DescribePlan().describe(exec.physicalPlan);
            return;
        }

        // Step 4: execute job
        if (line.hasOption("x"))
        {
            int id;
            try
            {
                id = Integer.parseInt(line.getOptionValue("x"));
            }
            catch (NumberFormatException e)
            {
                Map<Integer, String> matchedJobs = new HashMap<Integer, String>();
                String jobToRun = line.getOptionValue("x");
                id = 0;

                for (JsonNode job : exec.physicalPlan.get("jobs"))
                {
                    String jobName = JsonUtils.getText(job, "name");
                    if (jobName.contains(jobToRun))
                    {
                        matchedJobs.put(id, jobName);
                    }
                    id++;
                }

                if (matchedJobs.isEmpty())
                {
                    System.err.println("ERROR: There is no job that matches [" + jobToRun
                            + "]");
                    System.exit(1);
                }

                if (matchedJobs.size() > 1)
                {
                    System.err.println("ERROR: There are more than one jobs that matches ["
                            + jobToRun + "]:");
                    for (Map.Entry<Integer, String> entry : matchedJobs.entrySet())
                        System.err.println(String.format("\t[%d] %s",
                                                         entry.getKey(),
                                                         entry.getValue()));
                    System.exit(1);
                }

                id = matchedJobs.keySet().iterator().next();
            }

            exec.execute(id);
        }
        else
        {
            exec.execute();
        }
    }

    private static String readFile(File filename) throws IOException
    {
        return readFile(new FileInputStream(filename));
    }

    private static String readFile(InputStream in) throws IOException
    {
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
}
