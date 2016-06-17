Objective:
=========
1. Sometimes it would be good to understand the way files are being accessed in S3 (random, sequential seeks etc) to understand perf characteristics and also to replay those access patterns for perf tuning.  AWS-wrapper tries to capture these details in the log format which can feed into another program for replaying the access patterns.

2. Output of this wrapper can also be helpful in terms of determining connection leaks. It would be good to have the stacktrace of who made teh file open call (but that would be too much of an overhead currently, so capturing maily the ID for intial work).

Build:
=====
1. "mvn clean package".  This should create a hadoop-aws-wrapper-*.jar file in target folder.

Running:
========
1. Start hive cli as 'hive --hiveconf fs.s3a.impl="org.apache.hadoop.fs.s3a.wrapper.S3AWrapperFileSystem" '

2. In hive cli, "add jar file:///tmp/hadoop-aws-wrapper-2.7.1.jar;" (in case the jar in /tmp folder).

4. Run your queries as normal.

5. Get the logs by running yarn logs command. E.g
   "yarn logs -applicationId application_1465435117838_0247 | grep "S3AWrapper" > /tmp/stream_access_customer_query.log"


OOTB:
===
1. Attached here the pre-built hadoop-aws-wrapper-2.7.1.jar (in case of checking quickly without having to recompile)

2. In case connection leaks are there, there would not be any corresponding close() call in the log for the specific file. StackTraces can be added
later to find out from where the leak is created.

3. Attached "stream_access.log" which contains the sample access pattern of

	- "select count(*) from customer where c_customer_id between 'AAAAAAAAAADAAA' and 'AAAAAAAAABAAAAAA';"
	- "TPC-DS 200 GB query-27.sql"

4. Log format:

	For S3AWrapperFileSystem:
		hashCode_hashcode, filePath, operation, fileLen, timeInNanos
	For S3AWrapperInputStream:
		hashCode_hashCode, fileName, operation, fileLen, oldPos, currentPosAfterRead, positionalSeekLoc, bytesRead, timeInNanos
