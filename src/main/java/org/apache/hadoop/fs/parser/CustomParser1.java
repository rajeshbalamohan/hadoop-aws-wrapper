package org.apache.hadoop.fs.parser;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Used to parse logs from YARN logs. The data needs to be pre-prepared. FileNamePattenr:
 * ({query}.hashcodes} - combine all worker files into a single file - separate files per AM.
 *
 * Note: This is not parameterized, and code needs to be changed to point to the correct input
 * paths and prefixes.
 *
 */
public class CustomParser1 {

  void parseRuns() throws IOException {
    List<RunInfo> runInfos = new LinkedList<>();

    // TODO: Change prefixes and RunInfo to point to the correct files.
    // TODO: Eventually accept these as parameters.
    String []prefixes = new String[]{"q55am", "q56am", "q55execution", "q56execution"};
    runInfos.add(new RunInfo("runName", "path_containing_split_files"));

    // Make sure to fix parameters before running.
    System.err.println("Make sure to fix the above parameters, and commend out this section. Exiting.");
    System.exit(-1);

    for (String query : prefixes) {

      for (RunInfo runInfo : runInfos) {

        File dir = new File(runInfo.getDirectory());
        String runName = runInfo.getRunName();
        String[] listFiles = dir.list(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.toLowerCase().endsWith("hashcodes") && name.toLowerCase().contains(query);
          }
        });
        Preconditions.checkState(listFiles.length == 1,
            ("Failed for runInfo: " + runInfo + ", prefix=" + query));
        Arrays.sort(listFiles);

        for (String file : listFiles) {
          SingleFileParser parser = new SingleFileParser(new File(dir, file));
          parser.parse();
          StringBuilder sb = new StringBuilder();
          sb.append(runName + "," + file + ",");
          ParserUtils.computeDataReadPerMachine(parser.getPrasedData(), sb);
          ParserUtils.computeTimeTakenPerNode(parser.getPrasedData(), sb);
          for (String operation : parser.getOperations()) {
            ParserUtils.computeTimeTakenPerNode(operation, parser.getPrasedData(), sb);
          }
          // filesPerNode
          System.out.println(sb.toString());

        }
      }
    }
  }

  static class RunInfo {

    final String runName;
    final String directory;

    public RunInfo(String runName, String directory) {
      this.runName = runName;
      this.directory = directory;
    }

    public String getRunName() {
      return runName;
    }

    public String getDirectory() {
      return directory;
    }

    @Override
    public String toString() {
      return "RunInfo{" +
          "runName='" + runName + '\'' +
          ", directory='" + directory + '\'' +
          '}';
    }
  }

  public static void main(String[] args) throws IOException {
    CustomParser1 parser = new CustomParser1();
    parser.parseRuns();
  }
}