Build:
=====
1. "mvn clean package".  This should create a hadoop-aws-s3r jar file in target folder.

Running:
========
1. Start hive cli as 'hive --hiveconf fs.s3a.impl="org.apache.hadoop.fs.s3a.wrapper.S3AWrapperFileSystem" '
2. In hive cli, "ADD JAR file:///PATH_TO_YOUR_JAR/hadoop-aws-s3r-2.7.1.jar;"
    e.g "add jar file:///tmp/hadoop-aws-wrapper-2.7.1.jar;" (in case the jar in home folder).
4. Run your queries as normal.
5. Get the logs by running yarn logs command. E.g
   "yarn logs -applicationId application_1466066924279_0008 | grep "S3AWrapperInputStream" | grep "hashCode_" > stream_access.log"


OOTB:
===
1. Attached here the pre-built hadoop-aws-wrapper-2.7.1.jar (in case of checking quickly without having to recompile)
2. In case connection leaks are there, there would not be any corresponding close() call in the log for the specific file. StackTraces can be added
later to find out from where the leak is created.
3. Attached "stream_access.log" which contains the sample access pattern of "select count(*) from customer where c_customer_id like '%s3%';"
