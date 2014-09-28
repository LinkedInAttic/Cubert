
**Cubert is a fast and efficient batch computation engine for complex analysis and reporting of massive datasets on Hadoop**.

Cubert is a novel approach for batch computation on Hadoop:

1. A novel model of data organization into **Partitioned Data Units or blocks**.
	* One of the distinguishing feature of Cubert, compared to related data flow and relational paradigms is that rather than tuple-oriented processing, Cubert defines a notion of Block and applies operators to blocks.
	* Blocks are partitions of original data, are internally sorted, and generated using some cost function.
	* Cubert provides a new BLOCKGEN operator to organize data.
2. Set of **operators that are designed to be efficient**.
	* Includes new operators such as BLOCKGEN, PIVOT, CUBE, TEE and others.
	* Memory-efficient implementations of operators.
	* With the ability to write custom operators.
3. A concise **scripting language** and runtime to execute operators.
	* Can write Map-Reduce programs using simple scripting grammar, without touching any programming language code.
	* Provides flexibility and power to understand and control the flow of data.
	* Compatible with Pig User Defined Functions (UDFs).

Cubert has applications in several domains including Time range calculations, Complex join processing of large datasets, Statistics calculations and aggregations, OLAP Cubes and grouping sets, and Graph computations.


A flavor of Cubert Script
---------------------------------
Cubert script is a *physical* script where we explicitly define the operators at the Mappers, Reducers and Combiners for the different jobs. Following is an example of the Word Count problem written in cubert script.

	JOB "word count job"
        REDUCERS 10;
        MAP {
	            // load the input data set as a TEXT file
                input = LOAD "$CUBERT_HOME/examples/words.txt" USING TEXT("schema": "STRING word");
                // add a column to each tuple
                with_count = FROM input GENERATE word, 1 AS count;
        }
        // shuffle the data and also invoke combiner to aggregate on map-side
        SHUFFLE with_count PARTITIONED ON word AGGREGATES COUNT(count) AS count;
        REDUCE {
	            // at the reducers, sum the counts for each word
                output = GROUP with_count BY word AGGREGATES SUM(count) AS count;
        }
		// store the output using TEXT format
        STORE output INTO "output" USING TEXT();
	END

While the Cubert Script code above is already very concise representation of the Word Count problem; as a matter of interest, the idiomatic way of writing in Cubert is even more concise (and a lot faster)!

	JOB "idiomatic word count program (even more concise!)"
	        REDUCERS 10;
	        MAP {
	                input = LOAD "$CUBERT_HOME/examples/words.txt" USING TEXT("schema": "STRING word");
	        }
	        CUBE input BY word AGGREGATES COUNT(word) AS count GROUPING SETS (word);
	        STORE input INTO "output" USING TEXT();
	END

Installation
----------------
Download or clone the repository (say, into */path/to/cubert*) and run the following command:

	$ cd /path/to/cubert
	$ ./gradlew

This will create a folder */path/to/cubert/release*, which is what we will need to run cubert. This folder can be copied to hadoop cluster gateway.

To run cubert, first make sure that Hadoop is installed and the HADOOP\_HOME environment variable points to the hadoop installation. Set the CUBERT\_HOME environment to the release folder (note: CUBERT\_HOME points to the *release* folder and not the "root" repository folder).

	$ export CUBERT_HOME=/path/to/cubert/release
	$ $CUBERT_HOME/bin/cubert -h
		Using HADOOP_CLASSPATH=:/path/to/cubert/release/lib/*

		usage: ScriptExecutor <cubert script file> [options]
		 -c,--compile             stop after compilation
		 -D <property=value>      use value for given property
		 -d,--debug               print debuging information
		 ...
		
**Example Scripts**: Sample scripts are available in the $CUBERT\_HOME/examples folder.

Cubert Operators
-----------------
Cubert provides a rich suite of operators for processing data. These include:

* *Input/Output operators*: LOAD, STORE, TEE (storing data at intermediate stages of computation into a separate folder) and LOAD-CACHED (read from Hadoop's distributed cache).
* *Transformation operators*: FROM .. GENERATE (similar to SQL's SELECT), FILTER, LIMIT, TOPN (select top N rows from a window of rows), DISTINCT, SORT, DUPLICATE and FLATTEN (transform nested rows into flattened rows), MERGE JOIN and HASH JOIN.
* *Aggregation operators*: GROUP BY and CUBE (OLAP cube for additive and non-additive aggregates).
* *Data movement operators*: SHUFFLE (standard Hadoop shuffle), BLOCKGEN (create partitioned data units, aka blocks), COMBINE (combine two or more blocks), PIVOT (create sub-blocks out of a block, based on specified partitioning scheme).
* *Dictionary operators* to encode and decode strings to integers.

Coming (really) soon..
-------------------------
* **Tez execution engine**: the existing cubert scripts (written in Map-Reduce paradigm) can run *unmodified* on underlying Tez engine.
* **Cubert Script v2**: a DAG oriented language to express complex composition of workflows.
* **Incremental computations**: daily running jobs that read multiple days of data can materialize partial output and incrementally compute the results (thus cutting down on resources and time).
* **Analytical window functions**.
