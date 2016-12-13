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

package org.apache.hadoop.fs.gcs.wrapper;

import com.google.common.base.Throwables;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class GCSWrapperInputStream extends FSInputStream implements CanSetReadahead {
  private static final Logger LOG = LoggerFactory.getLogger(GCSWrapperInputStream.class);

  private final FSDataInputStream realStream;

  private final Path f;
  private final long contentLen;
  private final String address;
  private final boolean printStackTrace;

  public GCSWrapperInputStream(InputStream in, Path f, long contenLen) {
    this(in, f, contenLen, null, false);
  }

  public GCSWrapperInputStream(InputStream in, Path f, long contenLen,
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

  @Override public boolean seekToNewSource(long l) throws IOException {
    return realStream.seekToNewSource(l);
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
    log("close", oldPos, -1, -1, (end - start));
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
        + "," + msg
    );
  }
}

