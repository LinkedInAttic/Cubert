package com.linkedin.cubert.app;

import com.linkedin.cubert.analyzer.physical.*;
import com.linkedin.cubert.plan.physical.ExecutorService;
import com.linkedin.cubert.plan.physical.PhysicalParser;
import com.linkedin.cubert.utils.ExecutionConfig;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.RewriteUtils;
import org.apache.commons.cli.ParseException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Actual executor for Cubert CMR scripts.
 *
 * @author Mani Parkhe
 */
public class CmrExecutor
{
    private static String CUBERT_PROP_IDENTIFIER = "param.";

    private String program;
    private JsonNode physicalPlan;

    public CmrExecutor()
    {

    }

    public void run(ExecutionSpec spec, Properties props)
            throws Exception
    {
        program = spec.program;

        // Step 1: Preprocess the file
        if (props != null)
            preprocess(props);

        // command line option: if -s is set, show the code and exit
        if (spec.preprocessOnly)
        {
            System.out.println(program);
            return;
        }

        try
        {
            // Step 2: Translate to physical json plan
            parse();

            if (spec.parseOnly)
                return;

            // Step 3: compile plan
            compile(spec.debugMode);

            if (spec.compileOnly)
                return;
        }
        catch (Exception e)
        {
            if (e instanceof PlanRewriteException)
            {
                System.err.println(e.getMessage());

                if (spec.debugMode)
                    e.printStackTrace(System.err);
            }

            System.err.println("\nCannot parse cubert script. Exiting.");
            throw e;
        }
        finally
        {
            if (spec.printJson)
                printPhysical();
        }

        if (spec.describe)
        {
            new DescribePlan().describe(physicalPlan);
            return;
        }

        if (physicalPlan != null)
        {
            ((ObjectNode) physicalPlan).put("profileMode", spec.profileMode);
        }

        ExecutionConfig.getInstance().setParallelExec(spec.parallel);

        // Prepare for execution. Add paths to distributed cache
        if (spec.libJars != null)
        {
            ArrayNode libjars = (ArrayNode) physicalPlan.get("libjars");
            for (String jarPath : spec.libJars)
            {
                libjars.add(jarPath);
            }
        }

        // Step 4: execute jobs
        execute(spec.jobs);
    }

    private void preprocess(Properties props)
            throws
            InstantiationException,
            IllegalAccessException,
            ClassNotFoundException,
            IOException,
            InterruptedException,
            ParseException,
            ScriptException
    {
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
        matcher = Pattern.compile("<javascript>(.*?)</javascript>", Pattern.DOTALL)
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
                String cmd = String.format("var %s = \"%s\";",
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
                replaceVariables(scriptProps);
        }

        // The macro variables that are not already defined in the props, may be defined
        // as environment variables
        boolean needCubertTemp = false;
        for (String variable : variables)
        {
            if (!props.containsKey(variable))
            {
                String value = System.getenv(variable);
                if (value != null)
                    props.put(variable, value);
                else if (!needCubertTemp && variable.equals("CUBERT_TEMP"))
                    needCubertTemp = true;
            }
        }

        if (needCubertTemp)
        {
            Random rand = new Random();
            long suffix = 1;
            while (suffix < 100000L)
                suffix = Math.abs(rand.nextLong());

            String cubertTemp = String.format("CUBERT_TEMP__%d", suffix);

            System.out.println(
                    "variable CUBERT_TEMP not defined. Using value '" + cubertTemp + "'");

            props.put("CUBERT_TEMP", cubertTemp);
        }

        // Substitute the base variable
        if (props != null && props.size() > 0)
            replaceVariables(props);

        // Substitute backticks
        substituteBackticks();
    }

    private void replaceVariables(Properties props)
    {
        List<Object> keys = new ArrayList<Object>(props.keySet());
        Collections.sort(keys, new Comparator<Object>()
        {
            @Override public int compare(Object o1, Object o2)
            {
                String s1 = (String) o1;
                String s2 = (String) o2;
                return s2.length() - s1.length();
            }
        });

        for (Object key : keys)
        {
            program = program.replaceAll("\\$" + key,
                                         Matcher.quoteReplacement(((String) props.get(key))));
        }
    }

    private void substituteBackticks()
            throws IOException, InterruptedException
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

    /**
     * Parse the program and convert script into physical plan (json)
     *
     * @throws IOException
     * @throws java.text.ParseException
     */
    private void parse()
            throws IOException, java.text.ParseException
    {
        physicalPlan = PhysicalParser.parseProgram(program);
    }

    /**
     * Compile and Rewrite physical plan.
     *
     * @param debugMode
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void compile(boolean debugMode)
            throws IOException, InstantiationException, IllegalAccessException
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
        rewriters.add(BlockgenLineageAnalyzer.class);
        rewriters.add(SemanticAnalyzer.class);

        rewriters.add(SummaryRewriter.class);
        rewriters.add(OverwriteAnalyzer.class);
        rewriters.add(BlockgenLineageAnalyzer.class);
        rewriters.add(DependencyAnalyzer.class);
        rewriters.add(SemanticAnalyzer.class);

        HashSet<Class<? extends PlanRewriter>> visitedRewriters =
                new HashSet<Class<? extends PlanRewriter>>();

        for (Class<? extends PlanRewriter> rewriterClass : rewriters)
        {

            boolean revisit = (visitedRewriters.contains(rewriterClass) ? true : false);

            // revisit only performed, in the event of actual summary compile
            if (revisit && !RewriteUtils.hasSummaryRewrite((ObjectNode) physicalPlan))
                continue;

            physicalPlan = rewriterClass.newInstance()
                                        .rewrite(physicalPlan,
                                                 namesUsed,
                                                 debugMode,
                                                 revisit);
            visitedRewriters.add(rewriterClass);

            if (debugMode)
            {
                if (rewriterClass == SummaryRewriter.class
                        && RewriteUtils.hasSummaryRewrite((ObjectNode) physicalPlan))
                {
                    System.out.println("Physical plan after summary compile");
                    this.printPhysical();
                }

                if (rewriterClass == SemanticAnalyzer.class && revisit)
                {
                    System.out.println("Physical plan after semantic analyzer visit ");
                    this.printPhysical();
                }
            }

            if (physicalPlan == null)
                return;
        }

    }

    @SuppressWarnings("deprecation") private void printPhysical()
            throws IOException
    {
        System.out.println(new ObjectMapper().defaultPrettyPrintingWriter()
                                             .writeValueAsString(physicalPlan));
    }

    private void execute(List<String> jobs)
            throws
            IOException,
            InterruptedException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException
    {
        ExecutorService executorService = new ExecutorService(physicalPlan);

        if (jobs == null)
        {
            executorService.execute();
            return;
        }

        // else
        Set<Integer> jobsToRun = new TreeSet<Integer>();

        for (String idStr : jobs)
            jobsToRun.add(getJobId(idStr));

        for (int id : jobsToRun)
            executorService.execute(id);
    }

    private int getJobId(String idStr)
    {
        int id;
        try
        {
            id = Integer.parseInt(idStr);
        }
        catch (NumberFormatException e)
        {
            Map<Integer, String> matchedJobs = new HashMap<Integer, String>();
            String jobToRun = idStr;
            id = 0;

            for (JsonNode job : physicalPlan.get("jobs"))
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
                throw new IllegalStateException(
                        "ERROR: There is no job that matches [" + jobToRun + "]");
            }

            if (matchedJobs.size() > 1)
            {
                System.err.println(
                        "ERROR: There are more than one jobs that matches [" + jobToRun
                                + "]:");
                for (Map.Entry<Integer, String> entry : matchedJobs.entrySet())
                    System.err.println(String.format("\t[%d] %s",
                                                     entry.getKey(),
                                                     entry.getValue()));
                throw new IllegalStateException();
            }

            id = matchedJobs.keySet().iterator().next();
        }

        return id;
    }
}
