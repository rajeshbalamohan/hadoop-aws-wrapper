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

package org.apache.hadoop.fs.parser;

public class Data {
  private String hashCode;
  private String address;
  private String fileName;
  private String operation;
  private long contentLen;
  private long oldPos;
  private long realPos;
  private long positionalRead;
  private long read;
  private long timeInNanos;
  private String msg;

  public String getHashCode() {
    return hashCode;
  }

  public void setHashCode(String hashCode) {
    this.hashCode = hashCode;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public long getContentLen() {
    return contentLen;
  }

  public void setContentLen(long contentLen) {
    this.contentLen = contentLen;
  }

  public long getOldPos() {
    return oldPos;
  }

  public void setOldPos(long oldPos) {
    this.oldPos = oldPos;
  }

  public long getRealPos() {
    return realPos;
  }

  public void setRealPos(long realPos) {
    this.realPos = realPos;
  }

  public long getPositionalRead() {
    return positionalRead;
  }

  public void setPositionalRead(long positionalRead) {
    this.positionalRead = positionalRead;
  }

  public long getRead() {
    return read;
  }

  public void setRead(long read) {
    this.read = read;
  }

  public long getTimeInNanos() {
    return timeInNanos;
  }

  public void setTimeInNanos(long timeInNanos) {
    this.timeInNanos = timeInNanos;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  @Override
  public String toString() {
    return "Data{" +
        "hashCode='" + hashCode + '\'' +
        ", address='" + address + '\'' +
        ", fileName='" + fileName + '\'' +
        ", operation='" + operation + '\'' +
        ", contentLen=" + contentLen +
        ", oldPos=" + oldPos +
        ", realPos=" + realPos +
        ", positionalRead=" + positionalRead +
        ", read=" + read +
        ", timeInNanos=" + timeInNanos +
        ", msg='" + msg + '\'' +
        '}';
  }
}
