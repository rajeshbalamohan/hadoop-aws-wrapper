package org.apache.hadoop.fs.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class ParserUtils {

  public static long strToLong(String str) {
    try {
      return Long.parseLong(str);
    } catch (NumberFormatException npe) {
      return 0;
    }
  }

  /**
   * compute amount of data read per node
   */
  static void computeDataReadPerMachine(List<Data> dataList, StringBuilder sb) {
    Map<String, AtomicLong> dataReadPerNode = new HashMap<String, AtomicLong>();
    long count = 0;
    for (Data data : dataList) {
      String node = data.getAddress();
      if (!dataReadPerNode.containsKey(node)) {
        dataReadPerNode.put(node, new AtomicLong(0));
      }
      dataReadPerNode.get(node).addAndGet(data.getRead());
      count++;
    }
//    System.out.println("Data read per node : count=" + count);
//    prettyPrint(dataReadPerNode);
    long aggregate = getAggregate(dataReadPerNode);
    sb.append(count + "," + aggregate + "," + (aggregate / count));
  }

  /**
   * compute amount of time taken (overall including readFully, close etc)
   */
  static void computeTimeTakenPerNode(List<Data> dataList, StringBuilder sb) {
    computeTimeTakenPerNode(null, dataList, sb);
  }

  /**
   * compute amount of time taken based on filter
   */
  static void computeTimeTakenPerNode(String opsFilter, List<Data> dataList, StringBuilder sb) {
    final Map<String, AtomicLong> timeTakenPerNode = new TreeMap<>();

    long count = 0;
    for (Data data : dataList) {
      String node = data.getAddress();
      if (opsFilter != null && !opsFilter.isEmpty() && !data.getOperation().equals(opsFilter)) {
        continue;
      }
      if (!timeTakenPerNode.containsKey(node)) {
        timeTakenPerNode.put(node, new AtomicLong(0));
      }
      count++;
      timeTakenPerNode.get(node).addAndGet(data.getTimeInNanos());
    }
//    System.out.println("Time taken per node: operation(" + opsFilter + "): count=" + count);
//    prettyPrint(timeTakenPerNode);
    long aggregate = getAggregate(timeTakenPerNode);
    sb.append("," + count + "," + opsFilter + "," + aggregate + "," + (aggregate / count));
  }

  /**
   * Compute files processed per node
   */
  static void filesPerNode(List<Data> dataList, StringBuilder sb) {
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
//    prettyPrint(filesPerNode);
  }

  /**
   * Pretty print. Could use guava Joiner as well.
   */
  static void prettyPrint(Map<?, ?> map) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " --> " + entry.getValue());
    }
    System.out.println();
  }


  static long getAggregate(Map<?, AtomicLong> map) {
    long aggregate = 0l;
    for (Map.Entry<?, AtomicLong> entry : map.entrySet()) {
      aggregate += entry.getValue().get();
    }
    return aggregate;
  }

}
