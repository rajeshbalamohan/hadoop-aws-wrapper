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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * S3A Wrapper wchih logs all FS calls for future reference.
 */
public class S3AWrapperFileSystem extends FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(S3AWrapperFileSystem.class);
  private final S3AFileSystem realFS;

  @Override
  public URI getUri() {
    return realFS.getUri();
  }

  public S3AWrapperFileSystem() {
    super();
    realFS = new S3AFileSystem();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    realFS.initialize(name, conf);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    final FileStatus fileStatus = realFS.getFileStatus(f);
    FSDataInputStream rs = realFS.open(f, bufferSize);
    return new FSDataInputStream(new S3AWrapperInputStream(rs.getWrappedStream(), f, fileStatus
        .getLen()));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return realFS.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return realFS.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return realFS.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return realFS.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
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
    return realFS.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return realFS.getFileStatus(f);
  }
}

