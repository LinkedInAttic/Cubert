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

import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

/**
 * Utility methods to enumerate paths in the file system.
 * 
 * @author Maneesh Varshney
 * 
 */
public class FileSystemUtils
{
    public static List<Path> getPaths(FileSystem fs, JsonNode json) throws IOException
    {
        if (json.isArray())
        {
            List<Path> paths = new ArrayList<Path>();

            // If the specified input is array, recursively get paths for each item in the
            // array
            ArrayNode anode = (ArrayNode) json;
            for (int i = 0; i < anode.size(); i++)
            {
                paths.addAll(getPaths(fs, json.get(i)));
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
            JsonNode startDateJson = json.get("startDate");
            JsonNode endDateJson = json.get("endDate");
            long startDuration =
                    (startDateJson.isTextual())
                            ? Long.parseLong(startDateJson.getTextValue())
                            : startDateJson.getLongValue();
            long endDuration =
                    (endDateJson.isTextual())
                            ? Long.parseLong(endDateJson.getTextValue())
                            : endDateJson.getLongValue();

            boolean isDaily;
            if (Long.toString(startDuration).length() == 8)
            {
                if (Long.toString(endDuration).length() != 8)
                    throw new IllegalArgumentException("EndDate " + endDuration
                            + " is not consistent with StartDate " + startDuration);

                isDaily = true;
            }
            else if (Long.toString(startDuration).length() == 10)
            {
                if (Long.toString(endDuration).length() != 10)
                    throw new IllegalArgumentException("EndDate " + endDuration
                            + " is not consistent with StartDate " + startDuration);
                isDaily = false;
            }
            else
            {
                throw new IllegalArgumentException("Cannot parse StartDate "
                        + startDuration + " as daily or hourly duration");
            }

            for (Path path : getPaths(fs, root))
            {
                if (isDaily)
                    paths.addAll(getDailyDurationPaths(fs,
                                                       path,
                                                       startDuration,
                                                       endDuration));
                else
                    paths.addAll(getHourlyDurationPaths(fs,
                                                        path,
                                                        startDuration,
                                                        endDuration));
            }

            if (paths.isEmpty())
                throw new IOException(String.format("No input files at %s from %d to %d",
                                                    root.toString(),
                                                    startDuration,
                                                    endDuration));

            return paths;
        }

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

    public static List<Path> getDailyDurationPaths(FileSystem fs,
                                                   Path root,
                                                   long startDate,
                                                   long endDate) throws IOException
    {
        List<Path> paths = new ArrayList<Path>();

        for (FileStatus years : fs.listStatus(root))
        {
            int year = Integer.parseInt(years.getPath().getName());

            for (FileStatus months : fs.listStatus(years.getPath()))
            {
                int month = Integer.parseInt(months.getPath().getName());

                for (FileStatus days : fs.listStatus(months.getPath()))
                {
                    int day = Integer.parseInt(days.getPath().getName());

                    long timestamp = 10000L * year + 100L * month + day;
                    if (timestamp >= startDate && timestamp <= endDate)
                    {
                        paths.add(days.getPath());
                    }
                }
            }
        }

        return paths;
    }

    public static List<Path> getHourlyDurationPaths(FileSystem fs,
                                                    Path root,
                                                    long startDateHour,
                                                    long endDateHour) throws IOException
    {
        List<Path> paths = new ArrayList<Path>();

        for (FileStatus years : fs.listStatus(root))
        {
            int year = Integer.parseInt(years.getPath().getName());

            for (FileStatus months : fs.listStatus(years.getPath()))
            {
                int month = Integer.parseInt(months.getPath().getName());

                for (FileStatus days : fs.listStatus(months.getPath()))
                {
                    int day = Integer.parseInt(days.getPath().getName());

                    for (FileStatus hours : fs.listStatus(days.getPath()))
                    {
                        int hour = Integer.parseInt(hours.getPath().getName());

                        long timestamp =
                                1000000L * year + 10000L * month + 100L * day + hour;
                        if (timestamp >= startDateHour && timestamp <= endDateHour)
                        {
                            paths.add(hours.getPath());
                        }
                    }
                }
            }
        }

        return paths;
    }
}
