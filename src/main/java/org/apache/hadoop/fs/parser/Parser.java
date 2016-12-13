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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * yarn logs -applicationId application_1480722417364_0716 | grep "wrapper.GCSWrapperInputStream"
 * > output.txt
 * <p>
 * Rough code just for parsing logs. Inefficient way, but fine for getting quick info.
 * </p>
 */
public class Parser {

  static final String hashCode = "hashCode_";
  static final String inputStream = "InputStream";
  static final String fileSystem = "FileSystem";
  static List<Data> dataList = new LinkedList<Data>();

  public static long strToLong(String str) {
    try {
      return Long.parseLong(str);
    } catch (NumberFormatException npe) {
      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])))) {
      while (reader.ready()) {
        String line = reader.readLine();
        if (line.contains(hashCode)) {
          String useFulLine = line.substring(line.indexOf(hashCode), line.length()).trim();
          //TODO: Carry out some basic sanity checks here
          String[] contents = useFulLine.split(",");

          if (line.contains(inputStream)) {
            Data data = new Data();
            data.setHashCode(contents[0]);
            data.setAddress(contents[1]);
            data.setFileName(contents[2]);
            data.setOperation(contents[3]);
            data.setContentLen(strToLong(contents[4]));
            data.setOldPos(strToLong(contents[5]));
            data.setRealPos(strToLong(contents[6]));
            data.setPositionalRead(strToLong(contents[7]));
            data.setRead(strToLong(contents[8]));
            data.setTimeInNanos(strToLong(contents[9]));
            if (contents.length == 11) {
              data.setMsg(contents[10]);
            }
            dataList.add(data);
          }

          if (line.contains(fileSystem)) {
            Data data = new Data();
            data.setHashCode(contents[0]);
            data.setAddress(contents[1]);
            data.setFileName(contents[2]);
            data.setOperation(contents[3]);
            data.setContentLen(strToLong(contents[4]));
            data.setTimeInNanos(strToLong(contents[5]));
            dataList.add(data);
          }
        }
      }
      computeDataReadPerMachine();
      computeTimeTakenPerNode();
      computeTimeTakenPerNode("read");
      computeTimeTakenPerNode("readFully");
      computeTimeTakenPerNode("close");
      //FileSystem
      computeTimeTakenPerNode("open");
      filesPerNode();
    }
  }

  /**
   * compute amount of data read per node
   */
  static void computeDataReadPerMachine() {
    Map<String, Long> dataReadPerNode = new HashMap<String, Long>();
    long count = 0;
    for (Data data : dataList) {
      String node = data.getAddress();
      if (!dataReadPerNode.containsKey(node)) {
        dataReadPerNode.put(node, 0L);
      }
      dataReadPerNode.put(node, dataReadPerNode.get(node) + data.getRead());
      count++;
    }
    System.out.println("Data read per node : count=" + count);
    prettyPrint(dataReadPerNode);
  }

  /**
   * compute amount of time taken (overall including readFully, close etc)
   */
  static void computeTimeTakenPerNode() {
    computeTimeTakenPerNode(null);
  }

  /**
   * compute amount of time taken based on filter
   *
   * @param opsFilter
   */
  static void computeTimeTakenPerNode(String opsFilter) {
    final Map<String, Long> timeTakenPerNode = new TreeMap<>();
    long count = 0;
    for (Data data : dataList) {
      String node = data.getAddress();
      if (opsFilter != null && !opsFilter.isEmpty() && !data.getOperation().equals(opsFilter)) {
        continue;
      }
      if (!timeTakenPerNode.containsKey(node)) {
        timeTakenPerNode.put(node, 0L);
      }
      count++;
      timeTakenPerNode.put(node, timeTakenPerNode.get(node) + data.getTimeInNanos());
    }
    System.out.println("Time taken per node: operation(" + opsFilter + "): count=" + count);
    prettyPrint(timeTakenPerNode);
  }

  /**
   * Compute files processed per node
   */
  static void filesPerNode() {
    Map<String, String> filesPerNode = new TreeMap<String, String>();
    long count = 0;
    for (Data data : dataList) {
      String node = data.getAddress();
      if (!filesPerNode.containsKey(node)) {
        filesPerNode.put(node, "");
      }
      count++;
      filesPerNode.put(node, filesPerNode.get(node) + "," + data.getFileName());
    }
    System.out.println("Number of files read per node : count=" + count);
    prettyPrint(filesPerNode);
  }

  /**
   * Pretty print. Could use guava Joiner as well.
   *
   * @param map
   */
  static void prettyPrint(Map<?, ?> map) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " --> " + entry.getValue());
    }
    System.out.println();
  }
}
