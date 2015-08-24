package com.linkedin.cubert.app;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution spec for running Cubert CMR jobs
 *
 * @author Mani Parkhe
 */
public class ExecutionSpec
{
    // required minimums;
    final String program;
    List<String> libJars;
    List<String> jobs;

    boolean parallel;
    boolean profileMode;

    // execution stages
    boolean preprocessOnly;
    boolean parseOnly;
    boolean compileOnly;

    // non affecting settings
    boolean debugMode;
    boolean printJson;
    boolean describe;

    public ExecutionSpec(String program)
    {
        this.program = program;
        this.libJars = null;
        this.jobs = null;

        this.parallel = false;
        this.profileMode = false;

        this.preprocessOnly = false;
        this.parseOnly = false;
        this.compileOnly = false;

        this.debugMode = true;
        this.printJson = false;
        this.describe = false;
    }

    public ExecutionSpec addJar(Path jarPath)
    {
        if (libJars == null)
            libJars = new ArrayList<String>();

        libJars.add(jarPath.toString());
        return this;
    }

    public ExecutionSpec addJobs(String job)
    {
        if (jobs == null)
            jobs = new ArrayList<String>();

        jobs.add(job);
        return this;
    }

    public ExecutionSpec setParallel(boolean parallel)
    {
        this.parallel = parallel;
        return this;
    }

    public ExecutionSpec setProfileMode(boolean profileMode)
    {
        this.profileMode = profileMode;
        return this;
    }

    public ExecutionSpec setPreprocessOnly(boolean preprocessOnly)
    {
        this.preprocessOnly = preprocessOnly;
        return this;
    }

    public ExecutionSpec setParseOnly(boolean parseOnly)
    {
        this.parseOnly = parseOnly;
        return this;
    }

    public ExecutionSpec setCompileOnly(boolean compileOnly)
    {
        this.compileOnly = compileOnly;
        return this;
    }

    public ExecutionSpec setDebugMode(boolean debugMode)
    {
        this.debugMode = debugMode;
        return this;
    }

    public ExecutionSpec setPrintJson(boolean printJson)
    {
        this.printJson = printJson;
        return this;
    }

    public ExecutionSpec setDescribe(boolean describe)
    {
        this.describe = describe;
        return this;
    }
}
