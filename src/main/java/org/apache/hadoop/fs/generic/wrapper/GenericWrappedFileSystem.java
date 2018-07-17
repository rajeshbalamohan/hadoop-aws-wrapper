/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.generic.wrapper;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * GCS Wrapper which logs all FS calls for future reference.
 * <pre>
 *   e.g
 *   ./dist/hive/bin/hive --database rajesh --hiveconf fs.gs.impl=org.apache.hadoop.fs.gcs.wrapper.GCSWrapperFileSystem
 *   --hiveconf fs.gs.project.id=test-144806 --hiveconf fs.gs.working.dir=/
 *   --hiveconf fs.gs.auth.service.account.email=test@test-144806.iam.gserviceaccount.com
 *   --hiveconf fs.gs.auth.service.account.enable=true
 *   --hiveconf fs.gs.auth.service.account.keyfile=/tmp/test.p12
 *   --hiveconf hive.metastore.uris=""
 *   --hiveconf hive.tez.log.level=INFO
 *   --hiveconf tez.task.log.level=INFO
 *
 * hive> set hive.execution.mode=container;
 *
 * hive> add jar file:////home/rbalamohan/bigdata-interop/gcs/target/gcs-connector-1.5.6-hadoop2-SNAPSHOT-shaded.jar;
 * Added [file:////home/rbalamohan/bigdata-interop/gcs/target/gcs-connector-1.5.6-hadoop2-SNAPSHOT-shaded.jar] to class path
 * Added resources: [file:////home/rbalamohan/bigdata-interop/gcs/target/gcs-connector-1.5.6-hadoop2-SNAPSHOT-shaded.jar]
 *
 * hive> add jar file:///tmp/hadoop-aws-wrapper-2.7.0.jar;
 * Added [file:///tmp/hadoop-aws-wrapper-2.7.0.jar] to class path
 * Added resources: [file:///tmp/hadoop-aws-wrapper-2.7.0.jar]
 *
 * hive> select count(*) c from inventory group by inv_item_sk order by c limit 10;
 *
 * </pre>
 */
public abstract class GenericWrappedFileSystem extends FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(GenericWrappedFileSystem.class);
  private final FileSystem realFS;
  private final String address;
  private static final String PRINT_STACK_TRACE = "fs.wrapper.stacktrace";
  private boolean printStackTrace;

  @Override
  public URI getUri() {
    return realFS.getUri();
  }

  public GenericWrappedFileSystem(FileSystem realFs) {
    this.realFS = realFs;
    String localAddress = null;
    try {
      localAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      //ignore
    }
    this.address = localAddress;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    printStackTrace = conf.getBoolean(PRINT_STACK_TRACE, false);
    if (printStackTrace) {
      LOG.info("initialize.." + Throwables.getStackTraceAsString(new Exception()));
    }
    realFS.initialize(name, conf);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // TODO: This getFileStatus call is unnecessary except to log file size, and can cause a performance penalty
    final FileStatus fileStatus = realFS.getFileStatus(f);
    long startTime = System.nanoTime();
    FSDataInputStream rs = realFS.open(f, bufferSize);
    long endTime = System.nanoTime();
    log(f, "open", fileStatus.getLen(), (endTime - startTime));
    return new FSDataInputStream(new GenericWrappedInputStream(rs, f, fileStatus.getLen(), address,
        printStackTrace));
  }

  @Override public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws FileNotFoundException, IOException {
    LOG.info("Issued listLocaedStatus for " + f);
    long startTime = System.nanoTime();
    RemoteIterator<LocatedFileStatus> result = realFS.listLocatedStatus(f);
    long endTime = System.nanoTime();
    log(f, "listLocatedStatus", -1, (endTime - startTime));
    return result;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    long startTime = System.nanoTime();
    FSDataOutputStream out = realFS.create(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
    long endTime = System.nanoTime();
    log(f, "create", 0, (endTime - startTime));
    return out;
  }

  @Override
  public void close() throws IOException {
    //prints statistics if available
    LOG.info(realFS.toString());
    realFS.close();
    super.close();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    long startTime = System.nanoTime();
    FSDataOutputStream os = realFS.append(f, bufferSize, progress);
    long endTime = System.nanoTime();
    log(f, "append", -1, (endTime - startTime));
    return os;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename src=" + src + " to dest=" + dst);
    long startTime = System.nanoTime();
    boolean res = rename(src, dst);
    long endTime = System.nanoTime();
    log((src.toString() + "___" + dst.toString()), "rename", -1, endTime-startTime);
    return res;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.info("delete src=" + f + " recursive=" + recursive);
    long startTime = System.nanoTime();
    boolean res = realFS.delete(f, recursive);
    long endTime = System.nanoTime();
    log(f, "delete" + (recursive ? "_recursive" : "_nonrecursive"), -1, endTime-startTime);
    return res;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    if (printStackTrace) {
      LOG.info("listStatus path=" + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    long startTime = System.nanoTime();
    FileStatus[] res = realFS.listStatus(f);
    long endTime = System.nanoTime();
    log(f, "listStatus", (res == null ? -1 : res.length), (endTime - startTime));
    return res;
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    realFS.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return realFS.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    if (printStackTrace) {
      LOG.info("mkdirs path=" + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    long startTime = System.nanoTime();
    boolean res = realFS.mkdirs(f, permission);
    long endTime = System.nanoTime();
    log(f, "mkdirs", -1, endTime-startTime);
    return res;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (printStackTrace) {
      LOG.info(
          "getFileStatus path=" + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    long startTime = System.nanoTime();
    FileStatus fileStatus = realFS.getFileStatus(f);
    long endTime = System.nanoTime();
    log(f, "getFileStatus", (fileStatus == null ? -1 : fileStatus.getLen()), endTime-startTime);
    return fileStatus;
  }

  //Format: hashCode_hashcode, machine, filePath, operation, fileLen, timeInNanos
  private void log(Path f, String op, long contentLen, long timeInNanos) throws
      IOException {
    LOG.info("hashCode_" + hashCode()
        + "," + address
        + "," + f
        + "," + op
        + "," + contentLen
        + "," + timeInNanos
    );
  }

  //Format: hashCode_hashcode, machine, pathString, operation, fileLen, timeInNanos
  private void log(String pathString, String op, long contentLen, long timeInNanos) throws
      IOException {
    LOG.info("hashCode_" + hashCode()
        + "," + address
        + "," + pathString
        + "," + op
        + "," + contentLen
        + "," + timeInNanos
    );
  }
}

