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

package org.apache.hadoop.fs.s3a.wrapper;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * S3A Wrapper wchih logs all FS calls for future reference.
 */
public class S3AWrapperFileSystem extends FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(S3AWrapperFileSystem.class);
  private final S3AFileSystem realFS;
  private final String address;

  private static final String PRINT_STACK_TRACE = "fs.wrapper.stacktrace";
  private boolean printStackTrace;

  @Override
  public URI getUri() {
    return realFS.getUri();
  }

  public S3AWrapperFileSystem() {
    super();
    this.realFS = new S3AFileSystem();
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
    final FileStatus fileStatus = realFS.getFileStatus(f);
    long startTime = System.nanoTime();
    FSDataInputStream rs = realFS.open(f, bufferSize);
    long endTime = System.nanoTime();
    log(f, "open", fileStatus.getLen(), (endTime - startTime));
    return new FSDataInputStream(new S3AWrapperInputStream(rs.getWrappedStream(), f, fileStatus
        .getLen()));
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
    super.close();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return realFS.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename src=" + src + " to dest=" + dst);
    return realFS.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.info("delete src=" + f + " recursive=" + recursive);
    return realFS.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    if (printStackTrace) {
      LOG.info("listStatus path=" + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    return realFS.listStatus(f);
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
    return realFS.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (printStackTrace) {
      LOG.info("getFileStatus path=" + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    return realFS.getFileStatus(f);
  }

  //Format: hashCode_hashcode, address, filePath, operation, fileLen, timeInNanos
  private void log(Path f, String op, long contentLen,long timeInNanos) throws
      IOException {
    LOG.info("hashCode_" + hashCode()
        + "," + address
        + "," + f
        + "," + op
        + "," + contentLen
        + "," + timeInNanos
    );
  }
}

