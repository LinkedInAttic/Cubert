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

grammar CubertPhysical;
@header {
    package com.linkedin.cubert.antlr4;
}

program: programName? headerSection (job)* onCompletionTasks?;

programName: PROGRAM STRING ';';
headerSection: headerCommand*;
headerCommand: setCommand | registerCommand | functionDeclaration;
setCommand: SET uri constantExpression ';';
registerCommand: REGISTER path ';';
functionDeclaration: FUNCTION (alias=ID)? uri '(' functionArgs? ')' ';';
functionArgs: constantExpression (',' constantExpression)*;

job: createDictionary | mapReduceJob;


createDictionary: CREATEDICTIONARY ID (columnDictionary)+ ';';
columnDictionary: COLUMN ID VALUES STRING (',' STRING)*;

mapReduceJob: JOB
				name=STRING
				(MAPPERS mappersCount=INT ';')?
				(REDUCERS reducersCount=INT ';')?
				(mapCommands)+
				(macroShuffleCommand reduceCommands?)?
				outputCommand
			END;

mapCommands: MAP '{' inputCommand statement*  '}';

macroShuffleCommand: shuffleCommand
                     | blockgenShuffleCommand
                     | cubeShuffleCommand
                     | dictionaryShuffleCommand
                     | distinctShuffleCommand;

shuffleCommand: SHUFFLE ID PARTITIONEDON columns
                            (SORTEDON columns)?
                            (AGGREGATES aggregateList)? ';';
blockgenShuffleCommand: BLOCKGEN (distinct=DISTINCT)? ID by blockgenType=ID
                            (blockgenValue=INT | relation=path)?
                            PARTITIONEDON columns
                            (SORTEDON columns)? ';';
dictionaryShuffleCommand: DICTIONARY ID on columns ';';
distinctShuffleCommand: DISTINCT ID ';';

reduceCommands: REDUCE '{' statement+ '}';

cubeShuffleCommand: cubeStatement ';';

inputCommand: ID '=' LOAD inputPaths using (format=ID | classname=uri) ('(' params? ')')?';';
outputCommand: STORE ID INTO path using (format=ID | classname=uri) ('(' params? ')')?  ';';


statement: operatorCommand | multipassGroup;
multipassGroup: MULTIPASS singlePassGroup singlePassGroup+;
singlePassGroup: '{' operatorCommand+ '}';

operatorCommand: operatorCommandLhs '=' operator ';';
operatorCommandLhs: ID;

operator: noopOperator
		| encodeOperator
		| decodeOperator
		| filterOperator
		| groupByOperator
		| joinOperator
		| hashJoinOperator
		| loadBlockOperator
		| combineOperator
		| limitOperator
		| cubeOperator
		| duplicateOperator
		| generateOperator
		| flattenOperator
		| loadCachedOperator
		| distinctOperator
		| teeOperator
		| sortOperator
		| pivotOperator
		| gatherOperator
		| validateOperator
		| topNOperator
		| rankOperator
		| uriOperator
		;

noopOperator: NOOP ID (ASSERT PARTITIONEDON partitionKeys=columns SORTEDON sortKeys=columns)?;
encodeOperator: ENCODE ID on columns using (path | dictname=ID) (NULLS as nullas=ID)?;
decodeOperator: DECODE ID on columns using (path | dictname=ID);
filterOperator: FILTER ID by expression;
groupByOperator: GROUP ID by (ALL | columns) (AGGREGATES aggregateList)? (summaryRewriteClause) ?;
joinOperator: JOIN (joinType)? ID by columns ',' ID by columns ;
hashJoinOperator: HASHJOIN (joinType)? ID by columns ',' ID by columns ;
joinType: 'LEFT OUTER' | 'left outer' | 'RIGHT OUTER' | 'right outer' |'FULL OUTER' | 'full outer';
loadBlockOperator: LOADBLOCK (inmemory=INMEMORY)? FROM path MATCHING ID;
combineOperator: COMBINE ID (',' ID)* SORTEDON columns;
sortOperator: SORT ID on columns;
limitOperator: LIMIT ID INT;
cubeOperator: cubeStatement;
// cubeStatement is referenced by cubeOperator as well as cubeShuffleCommand
cubeStatement: CUBE ID by outer=columns (INNER inner=columns)? AGGREGATES cubeAggregateList (groupingSetsClause|groupingCombosClause|rollupsClause)? (HTSIZE htsize=INT)?;
duplicateOperator: DUPLICATE ID INT TIMES (COUNTERAS ID)?;
generateOperator: FROM ID GENERATE generateExpressionList;
flattenOperator: FLATTEN ID by flattenItem (',' flattenItem)*;
loadCachedOperator: LOADCACHED path using format=ID ('(' params? ')')?;
distinctOperator: DISTINCT ID;
teeOperator: TEE ID INTO path using ID ('(' params? ')')? (GENERATE generateExpressionList)? (IF expression)?;
pivotOperator: PIVOT (inmemory=INMEMORY)? ID (on columns) ? ;
gatherOperator: GATHER ID (',' ID)*;
validateOperator: VALIDATE ID by blockgenType=ID (blockgenValue=INT | index=path)? PARTITIONEDON columns  (SORTEDON columns)?;
topNOperator: TOP (n=INT)? FROM ID GROUP by group=columns ORDERBY order=columns;
rankOperator: RANK inputRelation=ID as rankColumn=ID (GROUP by group=columns ORDERBY order=columns)?;
uriOperator: uri idlist '{' params? '}';

groupingSetsClause: GROUPINGSETS cuboid (',' cuboid)*;
groupingCombosClause: GROUPINGCOMBOS n=INT;
rollupsClause: ROLLUPS cuboid (',' cuboid)*;
measure: column=ID (as outputName=ID)?;
cuboid: LBRACKET columns RBRACKET;

flattenItem: ID flattenType as typeDefinitions;
flattenType: TUPLE | BAG | BAGTUPLE;
typeDefinitions: '(' typeDefinition (',' typeDefinition)*   ')';
typeDefinition: ID ':' typeString=ID;


generateExpressionList: generateExpression (',' generateExpression)*;
generateExpression: expression (as ID)?;
expression:
      terminalExpression
    | LBRACKET expression RBRACKET
    | expression INOP LBRACKET expressionList RBRACKET
    | expression MULDIV expression
    | expression PLUSMINUS expression
    | expression BOOLEANOP expression
    | expression POSTSINGLEOP
    | PRESINGLEOP expression
    | expression ANDOR expression
    | uri LBRACKET expressionList? RBRACKET
    | CASE LBRACKET caseFunctionCallExpression RBRACKET;

terminalExpression:
        constantExpression
     |  columnProjectionExpression
     |  nestedProjectionExpression
     |  mapProjectionExpression;


functionCallExpression: expressionList;
expressionList: expression  (',' expression)*;

caseFunctionCallExpression: caseFunctionArg (',' caseFunctionArg)*;
caseFunctionArg: expression ',' expression;

constantExpression: INT | FLOAT | STRING | BOOLEAN;
columnProjectionExpression: '$' INT | ID;
nestedProjectionExpression: columnProjectionExpression '.' columnProjectionExpression ('.' columnProjectionExpression)*;
mapProjectionExpression:  columnProjectionExpression '#' STRING
                        | nestedProjectionExpression '#' STRING;



aggregateList: aggregate (',' aggregate)*;
aggregate: aggregationFunction '(' parameters=idlist? ')' (as ID)?;

aggregationFunction: ID | uri;

cubeAggregateList: cubeAggregate (',' cubeAggregate)*;
cubeAggregate: cubeAggregationFunction '(' parameters=idlist? ')' as ID;
cubeAggregationFunction: aggregationFunction | cubePartitionedAdditiveAggFunction;
cubePartitionedAdditiveAggFunction: '[' ID ',' ID ']';



onCompletionTasks: ONCOMPLETION '{' (onCompletionTask ';')* '}';
onCompletionTask: rmTask | mvTask | dumpTask | uriTask;
rmTask: RM path+;
mvTask: MV path path;
dumpTask: DUMP path;
uriTask: uri path+;

inputPaths: inputPath ( ',' inputPath)*;
inputPath: path | '(' path ',' INT ',' INT ')';
path: STRING;
columns: ID (',' ID)*;
summaryRewriteClause: 'SUMMARY VIEW=' ID ', SUMMARY PATH=' path ', TIME_COLUMNS=[' timeColumnSpec (',' timeColumnSpec) * ']';
timeColumnSpec: '(' factPath=path ',' dateColumn=uri ',' timeFormat= STRING ')';
params: keyval (',' keyval)*;
keyval: STRING ':' STRING;
uri: uriFragment ('.' uriFragment)*;
idlist: ID (',' ID)*;

uriFragment : ID
    | AGGREGATES
    | ALL
    | ASSERT
    | BAG
    | BAGTUPLE
    | BLOCKGEN
    | CASE
    | COLUMN
    | COMBINE
    | COUNTERAS
    | CUBE
    | DECODE
    | DEFAULTVALUE
    | DICTIONARY
    | DISTINCT
    | DUMP
    | DUPLICATE
    | ENCODE
    | END
    | FILTER
    | FLATTEN
    | FROM
    | FUNCTION
    | GATHER
    | GENERATE
    | GROUP
    | GROUPINGSETS
    | GROUPINGCOMBOS
    | ROLLUPS
    | HASHJOIN
    | HTSIZE
    | IF
    | INMEMORY
    | INNER
    | INTO
    | JOB
    | JOIN
    | LIMIT
    | LOAD
    | LOADBLOCK
    | LOADCACHED
    | MAP
    | MAPPERS
    | MATCHING
    | MULTIPASS
    | MV
    | NOOP
    | NULLS
    | ONCOMPLETION
    | ORDERBY
    | PARTITIONEDON
    | PIVOT
    | PROGRAM
    | REDUCE
    | REDUCERS
    | REGISTER
    | RM
    | SET
    | SHUFFLE
    | SORT
    | SORTEDON
    | STORE
    | TEE
    | TIMES
    | TOP
    | TUPLE
    | UNSPLITTABLE
    | USING
    | VALIDATE
    | VALUES;


// there is no reason why these are grammar rules rather than lexical tokens!
as: 'AS' | 'as';
on: 'ON' | 'on';
using: 'USING' | 'using';
by: 'BY' | 'by';

// Script keywords
AGGREGATES: 'AGGREGATES' | 'aggregates';
ALL: 'ALL' | 'all';
ASSERT: 'ASSERT' | 'assert';
BAG: 'BAG' | 'bag';
BAGTUPLE: 'BAG_TUPLE' | 'bag_tuple';
BLOCKGEN: 'BLOCKGEN' | 'blockgen';
CASE: 'CASE' | 'case';
COLUMN: 'COLUMN' | 'column';
COMBINE: 'COMBINE' | 'combine';
COUNTERAS: 'COUNTER AS' | 'counter as';
CREATEDICTIONARY: 'CREATE DICTIONARY' | 'create dictionary';
CUBE: 'CUBE' | 'cube';
DECODE: 'DECODE' | 'decode';
DEFAULTVALUE: 'DEFAULTVALUE' | 'defaultvalue';
DICTIONARY: 'DICTIONARY' | 'dictionary';
DISTINCT: 'DISTINCT' | 'distinct';
DUMP: 'dump' | 'DUMP';
DUPLICATE: 'DUPLICATE' | 'duplicate';
ENCODE: 'ENCODE' | 'encode';
END: 'END' | 'end';
FILTER: 'FILTER' | 'filter';
FLATTEN: 'FLATTEN' | 'flatten';
FROM: 'FROM' | 'from';
FUNCTION: 'FUNCTION' | 'function';
GATHER: 'GATHER' | 'gather';
GENERATE: 'GENERATE' | 'generate';
GROUP: 'GROUP' | 'group';
GROUPINGSETS: 'GROUPING SETS' | 'grouping sets';
GROUPINGCOMBOS: 'GROUPING COMBOS' | 'grouping combos';
ROLLUPS: 'ROLLUPS' | 'rollups';
HASHJOIN: 'HASH-JOIN' | 'hash-join';
HTSIZE: 'HTSIZE' | 'htsize';
IF: 'IF' | 'if';
INMEMORY: 'IN MEMORY' | 'in memory';
INNER: 'INNER' | 'inner';
INTO: 'INTO' | 'into';
JOB: 'JOB' | 'job';
JOIN: 'JOIN' | 'join';
LIMIT: 'LIMIT' | 'limit';
LOAD: 'LOAD' | 'load';
LOADBLOCK: 'LOAD BLOCK' | 'load block';
LOADCACHED: 'LOAD-CACHED' | 'load-cached';
MAP: 'MAP' | 'map';
MAPPERS: 'MAPPERS' | 'mappers';
MATCHING: 'MATCHING' | 'matching';
MULTIPASS: 'MULTIPASS' | 'multipass';
MV: 'MV' | 'mv';
NOOP: 'NO_OP' | 'no_op';
NULLS: 'NULLS' | 'nulls';
ONCOMPLETION: 'onCompletion' | 'oncompletion' | 'OnCompletion' | 'ONCOMPLETION';
ORDERBY: 'ORDER BY' | 'order by';
PARTITIONEDON: 'PARTITIONED ON' | 'partitioned on';
PIVOT: 'PIVOT' | 'pivot';
PROGRAM: 'PROGRAM' | 'program';
RANK: 'RANK' | 'rank';
REDUCE: 'REDUCE' | 'reduce';
REDUCERS: 'REDUCERS' | 'reducers';
REGISTER: 'REGISTER' | 'register';
RM: 'RM' | 'rm';
SET: 'SET' | 'set';
SHUFFLE: 'SHUFFLE' | 'shuffle';
SORT: 'SORT' | 'sort';
SORTEDON: 'SORTED ON' | 'sorted on';
STORE: 'STORE' | 'store';
TEE: 'TEE' | 'tee';
TIMES: 'TIMES' | 'times';
TOP: 'TOP' | 'top';
TUPLE: 'TUPLE' | 'tuple';
UNSPLITTABLE: 'UNSPLITTABLE' | 'unsplittable';
USING: 'USING' | 'using';
VALIDATE: 'VALIDATE' | 'validate';
VALUES: 'VALUES' | 'values';


MULDIV: '*' | '/';
PLUSMINUS:'+' | '-';
ANDOR : 'AND' |'and' | 'OR' | 'or';
LBRACKET: '(';
RBRACKET: ')';
BOOLEANOP : '>' | '>=' | '<' | '<=' | '==' |  '!=' | 'MATCHES' | 'matches';
INOP: 'IN' | 'in';
PRESINGLEOP: 'NOT' | 'not';
POSTSINGLEOP: 'IS NULL' | 'IS NOT NULL' | 'is null' | 'is not null';
BOOLEAN: 'true' | 'false';


ID: LETTER (LETTER | Digit)* ;
fragment LETTER : 'a'..'z'|'A'..'Z'|'_'  ;

STRING : '"' ('\\"'|.)*? '"' ;



// Copied from Java Antlr Grammar
INT
    :   Sign? DecimalIntegerLiteral
    |   Sign? HexIntegerLiteral
    |   Sign? OctalIntegerLiteral
    ;

fragment
DecimalIntegerLiteral
    :   DecimalNumeral IntegerTypeSuffix?
    ;

fragment
HexIntegerLiteral
    :   Sign? HexNumeral IntegerTypeSuffix?
    ;

fragment
OctalIntegerLiteral
    :   OctalNumeral IntegerTypeSuffix?
    ;

fragment
IntegerTypeSuffix
    :   [lL]
    ;

fragment
DecimalNumeral
    :   '0'
    |   NonZeroDigit (Digits? | Underscores Digits)
    ;

fragment
Digits
    :   Digit (DigitOrUnderscore* Digit)?
    ;

fragment
Digit
    :   '0'
    |   NonZeroDigit
    ;

fragment
NonZeroDigit
    :   [1-9]
    ;

fragment
DigitOrUnderscore
    :   Digit
    |   '_'
    ;

fragment
Underscores
    :   '_'+
    ;

fragment
HexNumeral
    :   '0' [xX] HexDigits
    ;

fragment
HexDigits
    :   HexDigit (HexDigitOrUnderscore* HexDigit)?
    ;

fragment
HexDigit
    :   [0-9a-fA-F]
    ;

fragment
HexDigitOrUnderscore
    :   HexDigit
    |   '_'
    ;

fragment
OctalNumeral
    :   '0' Underscores? OctalDigits
    ;

fragment
OctalDigits
    :   OctalDigit (OctalDigitOrUnderscore* OctalDigit)?
    ;

fragment
OctalDigit
    :   [0-7]
    ;

fragment
OctalDigitOrUnderscore
    :   OctalDigit
    |   '_'
    ;


FLOAT
    :   Sign? DecimalFloatingPointLiteral
    |   Sign? HexadecimalFloatingPointLiteral
    ;

fragment
DecimalFloatingPointLiteral
    :   Digits '.' Digits? ExponentPart? FloatTypeSuffix?
    |   '.' Digits ExponentPart? FloatTypeSuffix?
    |   Digits ExponentPart FloatTypeSuffix?
    |   Digits FloatTypeSuffix
    ;

fragment
ExponentPart
    :   ExponentIndicator SignedInteger
    ;

fragment
ExponentIndicator
    :   [eE]
    ;

fragment
SignedInteger
    :   Sign? Digits
    ;

fragment
Sign
    :   [+-]
    ;

fragment
FloatTypeSuffix
    :   [fFdD]
    ;

fragment
HexadecimalFloatingPointLiteral
    :   HexSignificand BinaryExponent FloatTypeSuffix?
    ;

fragment
HexSignificand
    :   HexNumeral '.'?
    |   '0' [xX] HexDigits? '.' HexDigits
    ;

fragment
BinaryExponent
    :   BinaryExponentIndicator SignedInteger
    ;

fragment
BinaryExponentIndicator
    :   [pP]
    ;
// END copy from Java Antlr grammar


LINE_COMMENT : '//' .*? '\r'? '\n' -> skip ;
COMMENT : '/*' .*? '*/' -> skip ;
// WS: [ \t\n\r]+ -> skip;
WS: [ \t\n\r]+ -> channel(HIDDEN);
