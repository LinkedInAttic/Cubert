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

package com.linkedin.cubert.utils;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.linkedin.cubert.utils.JsonUtils.getText;

/**
 * Utility methods to enumerate paths in the file system.
 * 
 * @author Maneesh Varshney
 * 
 */
public class FileSystemUtils
{

    public static Path getFirstMatch(FileSystem fs, Path path, String globPatternStr, boolean recursive)
        throws IOException
    {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, recursive);
        GlobPattern globPattern = new GlobPattern(globPatternStr);

        while (files.hasNext())
        {
            Path aFile = files.next().getPath();
            if(globPattern.matches(aFile.getName()))
                return aFile;
        }

        return null;
    }

     public static List<Path> getPaths(FileSystem fs, JsonNode json, JsonNode params) throws IOException {
      return getPaths(fs, json, false, params);
    }

    public static List<Path> getPaths(FileSystem fs, JsonNode json,
                                      boolean schemaOnly, JsonNode params) throws IOException
    {
        if (json.isArray())
        {
            List<Path> paths = new ArrayList<Path>();
            // If the specified input is array, recursively get paths for each item in the
            // array
            ArrayNode anode = (ArrayNode) json;
            for (int i = 0; i < anode.size(); i++)
            {
                paths.addAll(getPaths(fs, json.get(i), params));
            }
            return paths;
        }
        else if (json.isTextual())
        {
            return getPaths(fs, new Path(json.getTextValue()));
        }
        else
        {
            List<Path> paths = new ArrayList<Path>();
            Path root = new Path(getText(json, "root"));
            Path basePath = root;
            JsonNode startDateJson = json.get("startDate");
            if (schemaOnly && json.get("origStartDate") != null)
              startDateJson = json.get("origStartDate");

            JsonNode endDateJson = json.get("endDate");
            if(startDateJson == null || endDateJson == null)
            {
                throw new IllegalArgumentException("StartDate and endDate need to be specified");
            }
            String startDuration, endDuration;
            if(startDateJson.isTextual())
            {
                startDuration = startDateJson.getTextValue();
                endDuration = endDateJson.getTextValue();
            }

            else
            {
                startDuration = startDateJson.toString();
                endDuration = endDateJson.toString();
            }

            boolean errorOnMissing = false;
            JsonNode errorOnMissingJson = params.get("errorOnMissing");
            if(errorOnMissingJson != null)
                errorOnMissing = Boolean.parseBoolean(errorOnMissingJson.getTextValue());

            boolean useHourlyForMissingDaily = false;
            JsonNode useHourlyForMissingDailyJson = params.get("useHourlyForMissingDaily");
            if(useHourlyForMissingDailyJson != null)
                useHourlyForMissingDaily = Boolean.parseBoolean(useHourlyForMissingDailyJson.getTextValue());


            DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd");
            DateTimeFormatter dtfwHour = DateTimeFormat.forPattern("yyyyMMddHH");
            DateTime startDate, endDate;
            boolean isDaily;
            int hourStep;
            if (startDuration.length() == 8)
            {
                if (endDuration.length() != 8)
                    throw new IllegalArgumentException("EndDate " + endDuration
                            + " is not consistent with StartDate " + startDuration);
                startDate = dtf.parseDateTime(startDuration);
                endDate = dtf.parseDateTime(endDuration);
                isDaily = true;
                hourStep = 24;
            }
            else if (startDuration.length() == 10)
            {
                if (endDuration.length() != 10)
                    throw new IllegalArgumentException("EndDate " + endDuration
                            + " is not consistent with StartDate " + startDuration);
                startDate = dtfwHour.parseDateTime(startDuration);
                endDate = dtfwHour.parseDateTime(endDuration);
                isDaily = false;
                hourStep = 1;
            }
            else
            {
                throw new IllegalArgumentException("Cannot parse StartDate "
                        + startDuration + " as daily or hourly duration");

            }

            for(Path path: getPaths(fs,root))
            {
                if(isDaily)
                {
                    if(path.getName().equals("daily"))
                        basePath = path;
                    else
                        basePath = new Path(path, "daily");
                }
                else
                {
                    if(path.getName().equals("hourly"))
                        basePath = path;
                    else
                        basePath = new Path(path, "hourly");
                }

                //If daily folder itself doesn't exist
                if (!fs.exists(basePath) && isDaily && useHourlyForMissingDaily &&
                        fs.exists(new Path(basePath.getParent(), "hourly"))) {
                    basePath = new Path(basePath.getParent(), "hourly");
                    endDate = endDate.plusHours(23);
                    isDaily = false;
                    hourStep = 1;
                }

                paths.addAll(getDurationPaths(fs,
                        basePath,
                        startDate,
                        endDate,
                        isDaily,
                        hourStep,
                        errorOnMissing,
                        useHourlyForMissingDaily));
            }

            if (paths.isEmpty() && schemaOnly)
                throw new IOException(String.format("No input files at %s from %s to %s",
                                                    basePath.toString(),
                                                    startDuration,
                                                    endDuration));
            return paths;
        }

    }

    private static Path generateDatedPath(Path base, int year, int month, int day) {
        return generateDatedPath(base, year, month, day, -1);
    }

    private static Path generateDatedPath(Path base, int year, int month, int day, int hour) {
        NumberFormat nf2 = new DecimalFormat("00");
        return new Path(base, hour != -1 ? nf2.format(year) + "/" + nf2.format(month) + "/" + nf2.format(day) + "/"
                + nf2.format(hour) : nf2.format(year) + "/" + nf2.format(month) + "/" + nf2.format(day));
    }

    public static List<Path> getPaths(FileSystem fs, Path path) throws IOException
    {
        List<Path> paths = new ArrayList<Path>();


        String pathStr = path.toString();

        if (pathStr.contains("*"))
        {
            for (Path p : getGlobPaths(fs, path))
            {
                paths.add(getLatestPath(fs, p));
            }
        }
        else
        {
            paths.add(getLatestPath(fs, path));
        }

        return paths;
    }

    public static List<Path> getGlobPaths(FileSystem fs, Path path) throws IOException
    {
        List<Path> paths = new ArrayList<Path>();

        FileStatus[] fileStatus = fs.globStatus(path);

        if (fileStatus == null)
            throw new IOException("Cannot determine paths at " + path.toString());

        for (FileStatus status : fileStatus)
        {
            paths.add(status.getPath());
        }

        return paths;
    }

    public static Path getLatestPath(FileSystem fs, Path path) throws IOException
    {
        String pathStr = path.toString();

        // Return the same path, if there is no "#LATEST" within it
        if (!pathStr.contains("#LATEST"))
            return path;

        // replace all #LATEST with glob "*"
        pathStr = pathStr.replaceAll("#LATEST", "*");

        FileStatus[] fileStatus = fs.globStatus(new Path(pathStr));

        if (fileStatus == null || fileStatus.length == 0)
            throw new IOException("Cannot determine paths at " + pathStr);

        String latestPath = null;
        for (FileStatus status : fileStatus)
        {
            String thisPath = status.getPath().toString();
            if (latestPath == null || thisPath.compareTo(latestPath) > 0)
                latestPath = thisPath;

        }
        return new Path(latestPath);
    }

    public static List<Path> getDurationPaths(FileSystem fs,
                                              Path root,
                                              DateTime startDate,
                                              DateTime endDate,
                                              boolean isDaily,
                                              int hourStep,
                                              boolean errorOnMissing,
                                              boolean useHourlyForMissingDaily) throws IOException
    {
        List<Path> paths = new ArrayList<Path>();
        while (endDate.compareTo(startDate) >= 0) {
            Path loc;
            if (isDaily)
                loc = generateDatedPath(root, endDate.getYear(), endDate.getMonthOfYear(), endDate.getDayOfMonth());
            else
                loc = generateDatedPath(root, endDate.getYear(), endDate.getMonthOfYear(), endDate.getDayOfMonth(),
                        endDate.getHourOfDay());

            // Check that directory exists, and contains avro files.
            if (fs.exists(loc) && fs.globStatus(new Path(loc, "*" + "avro")).length > 0) {
                paths.add(loc);
            }

            else {

                loc = generateDatedPath(new Path(root.getParent(),"hourly"), endDate.getYear(),
                        endDate.getMonthOfYear(), endDate.getDayOfMonth());
                if(isDaily && useHourlyForMissingDaily && fs.exists(loc))
                {
                      for (FileStatus hour: fs.listStatus(loc)) {
                          paths.add(hour.getPath());
                      }
                }

                else if (errorOnMissing) {
                    throw new RuntimeException("Missing directory " + loc.toString());
                }

            }
            if (hourStep ==24)
                endDate = endDate.minusDays(1);
            else
                endDate = endDate.minusHours(hourStep);
        }
        return paths;
    }

}
