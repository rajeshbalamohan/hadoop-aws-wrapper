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

package org.apache.hadoop.fs.hdfs.wrapper;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper which logs all read calls in the following format
 * <p>
 * hashCode_<hashCode>, fileName, operation, contentLengthOfFile,
 * positionBeforeRead, positionAfterRead, positionalSeekLoc, bytesRead, timeTakenInNanos
 * <p>
 * This would be logged in normal job log. So one can filter out
 * yarn logs -applicationId appId | grep "S3AWrapper" > stream.log
 * <p>
 * This can be parsed and played back later for reproducing the access
 * pattern later (e.g TPC-DS workload or TPC-H workload).
 *
 * Note: readFully is from DataInputStream and is marked final. So
 * it is hard to get that detail here. However, readFully internally
 * makes read calls which are captured. Only issue is, it is possible
 * that AWS is returning them in chunks (e.g trying to do readFully of 4 MB
 * might be done via 2 or 3 read operations).
 *
 * later point, hashCode can be used to find out any means of connection leaks.
 * StackTrace is too much to add now.
 */
public class HdfsWrapperInputStream extends FSInputStream implements CanSetReadahead {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsWrapperInputStream.class);

  private final FSInputStream realStream;

  private final Path f;
  private final long contentLen;
  private final String address;
  private final boolean printStackTrace;

  public HdfsWrapperInputStream(InputStream in, Path f, long contentLen) {
    this(in, f, contentLen, null, false);
  }

  public HdfsWrapperInputStream(InputStream in, Path f, long contentLen,
                               String address, boolean printStackTrace) {
//    Preconditions.checkArgument(in instanceof S3AInputStream,
//        "Not an instance of S3AInputStream; "
//            + in.getClass().toString());
    this.realStream =  (FSInputStream)in;
    LOG.info(("RealStream class: " + realStream.getClass()));
    this.f = f;
    this.contentLen = contentLen;
    this.address = address;
    this.printStackTrace = printStackTrace;
  }

  @Override
  public void setReadahead(Long readahead)
      throws IOException, UnsupportedOperationException {
    //TODO: ignore for now. Add it later.
    // realStream.setReadahead(readahead);
  }

  @Override
  public void seek(long pos) throws IOException {
    realStream.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return realStream.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    boolean seek = seekToNewSource(targetPos);
    return seek;
  }

  @Override
  public int read() throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read();
    long end = System.nanoTime();
    log("read", oldPos, -1, read, (end - start));
    return read;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read(b, off, len);
    long end = System.nanoTime();
    log("read", oldPos, -1, read, (end - start));
    return read;
  }

  @Override
  public void close() throws IOException {
    LOG.info(realStream.toString());
    long oldPos = realStream.getPos();
    long start = System.nanoTime();
    realStream.close();
    long end = System.nanoTime();
    log("close", oldPos, -1, -1, (end-start));
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    realStream.readFully(position, buffer, offset, length);
    long end = System.nanoTime();
    log("readFully", oldPos, position, length, (end - start));
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    realStream.readFully(position, buffer);
    long end = System.nanoTime();
    log("readFully", oldPos, position, buffer.length, (end - start));
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read(position, buffer, offset, length);
    long end = System.nanoTime();
    log("read", oldPos, position, read, (end - start));
    return read;
  }

  @Override
  public int read(byte[] b) throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read(b);
    long end = System.nanoTime();
    log("read", oldPos, -1, read, (end - start));
    return read;
  }

  //Format: hashCode_hashCode, address, fileName, operation, fileLen, oldPos, currentPosAfterRead,
  // bytesRead, timeInNanos
  private void log(String op, long oldPos, long positionalRead, int read, long timeInNanos) throws
      IOException {
    String msg = "";
    if (read == 0 || read == 1) {
      if (printStackTrace) {
        msg = Throwables.getStackTraceAsString(new Exception());
      }
    }
    LOG.info("hashCode_" + hashCode()
        + "," + address
        + "," + f
        + "," + op
        + "," + contentLen
        + "," + oldPos
        + "," + realStream.getPos()
        + "," + positionalRead // only applicable if someone is requesting for specific pos read
        + "," + read
        + "," + timeInNanos
        + "," + msg
    );
  }
}

