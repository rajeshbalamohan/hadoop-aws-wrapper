/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.parser;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * yarn logs -applicationId application_1480722417364_0716 | grep "wrapper.GCSWrapperInputStream" >
 * output.txt
 * <p>
 * Rough code just for parsing logs. Inefficient way, but fine for getting quick info.
 * </p>
 */
public class Parser {


  static List<Data> dataList = new LinkedList<Data>();

  static Set<String> operations = new HashSet<>();
  static StringBuilder sb = new StringBuilder();

  public static long strToLong(String str) {
    try {
      return Long.parseLong(str);
    } catch (NumberFormatException npe) {
      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    String fileString = args[0];
    SingleFileParser parser = new SingleFileParser(fileString);
    dataList.addAll(parser.getPrasedData());
    operations.addAll(parser.getOperations());
    ParserUtils.computeDataReadPerMachine(dataList, sb);
    ParserUtils.computeTimeTakenPerNode(dataList, sb);
    for (String operation : operations) {
      ParserUtils.computeTimeTakenPerNode(operation, dataList, sb);
    }
    // filesPerNode
    System.out.println(sb.toString());
  }
}
