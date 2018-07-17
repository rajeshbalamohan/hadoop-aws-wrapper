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
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSDataInputStream;
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
public class GenericWrappedInputStream extends FSInputStream implements CanSetReadahead {
  private static final Logger LOG = LoggerFactory.getLogger(GenericWrappedInputStream.class);

  private final FSDataInputStream realStream;

  private final Path f;
  private final long contentLen;
  private final String address;
  private final boolean printStackTrace;

  public GenericWrappedInputStream(InputStream in, Path f, long contenLen) {
    this(in, f, contenLen, null, false);
  }

  public GenericWrappedInputStream(InputStream in, Path f, long contenLen,
      String address, boolean printStackTrace) {
    this.realStream = (FSDataInputStream) in;
    this.f = f;
    this.contentLen = contenLen;
    this.address = address;
    this.printStackTrace = printStackTrace;
    if (printStackTrace) {
      LOG.info("Creating new input stream.." + Throwables.getStackTraceAsString(new Exception()));
    }
  }

  @Override
  public void setReadahead(Long readahead)
      throws IOException, UnsupportedOperationException {
    //TODO: ignore for now. Add it later.
    // TODO: See which filesystem implementations support this, and add to the relevant ones.
    // realStream.setReadahead(readahead);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (printStackTrace) {
      LOG.info("seek" + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    long start = System.nanoTime();
    realStream.seek(pos);
    long end = System.nanoTime();
    log("seek", pos, -1, -1, (end - start));
  }

  @Override
  public long getPos() throws IOException {
    long start = System.nanoTime();
    long pos = realStream.getPos();
    long end = System.nanoTime();
    log("getPos", -1, -1, -1, (end - start));
    return pos;
  }

  @Override public boolean seekToNewSource(long l) throws IOException {
    long start = System.nanoTime();
    boolean res = realStream.seekToNewSource(l);
    long end = System.nanoTime();
    log("seekToNewSource", -1, -1, -1, (end - start));
    return res;
  }

  @Override
  public int read() throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read();
    long end = System.nanoTime();
    log("read1", oldPos, -1, read, (end - start));
    return read;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read(b, off, len);
    long end = System.nanoTime();
    log("read2", oldPos, -1, read, (end - start));
    return read;
  }

  @Override
  public void close() throws IOException {
    LOG.info(realStream.toString());
    long oldPos = realStream.getPos();
    long start = System.nanoTime();
    realStream.close();
    long end = System.nanoTime();
    log("close", oldPos, -1, -1, (end - start));
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if (printStackTrace) {
      LOG.info("readFully1 " + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    realStream.readFully(position, buffer, offset, length);
    long end = System.nanoTime();
    log("readFully1", oldPos, position, length, (end - start));
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    if (printStackTrace) {
      LOG.info("readFully2 " + f + ", " + Throwables.getStackTraceAsString(new Exception()));
    }
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    realStream.readFully(position, buffer);
    long end = System.nanoTime();
    log("readFully2", oldPos, position, buffer.length, (end - start));
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read(position, buffer, offset, length);
    long end = System.nanoTime();
    log("read3", oldPos, position, read, (end - start));
    return read;
  }

  @Override
  public int read(byte[] b) throws IOException {
    long start = System.nanoTime();
    long oldPos = realStream.getPos();
    int read = realStream.read(b);
    long end = System.nanoTime();
    log("read4", oldPos, -1, read, (end - start));
    return read;
  }

  //Format: hashCode_hashCode, fileName, operation, fileLen, oldPos, currentPosAfterRead,
  // bytesRead, timeInNanos
  private void log(String op, long oldPos, long positionalRead, int read, long timeInNanos) throws
      IOException {
    String msg = "";
    if (read == 0 || read == 1) {
      if (printStackTrace) {
        msg = Throwables.getStackTraceAsString(new Exception());
      }
    }
    long realPos = -100000;
    if (realStream != null) {
      try {
        realPos = realStream.getPos();
      } catch (Throwable t) {
        //in case it is already closed, it would throw exception. ignore
      }
    }
    LOG.info("hashCode_" + hashCode()
        + "," + address
        + "," + f
        + "," + op
        + "," + contentLen
        + "," + oldPos
        + "," + realPos
        + "," + positionalRead // only applicable if someone is requesting for specific pos read
        + "," + read
        + "," + timeInNanos
        + (msg.equals("") ? "" : "," + msg)
    );
  }
}

