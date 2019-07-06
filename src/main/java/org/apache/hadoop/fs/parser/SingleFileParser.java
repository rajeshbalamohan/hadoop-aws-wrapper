package org.apache.hadoop.fs.parser;

import static org.apache.hadoop.fs.parser.ParserUtils.strToLong;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SingleFileParser {

  private static final String hashCode = "hashCode_";
  private static final String inputStream = "InputStream";
  private static final String fileSystem = "FileSystem";

  private final List<Data> dataList = new LinkedList<>();
  private final Set<String> operations = new HashSet<>();
  private final File file;

  private boolean parsed = false;

  public SingleFileParser(String fileName) {
    this(new File(fileName));
  }

  public SingleFileParser(File file) {
    this.file = file;
  }

  public List<Data> getPrasedData() {
    Preconditions.checkState(parsed == true, "Must call parse before accessing parsed data");
    return dataList;
  }

  public Set<String> getOperations() {
    Preconditions.checkState(parsed == true, "Must call parse before accessing parsed data");
    return operations;
  }

  public void parse() throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
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
            operations.add(contents[3]);
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
          } else if (line.contains(fileSystem)) {
            Data data = new Data();
            data.setHashCode(contents[0]);
            data.setAddress(contents[1]);
            data.setFileName(contents[2]);
            operations.add(contents[3]);
            data.setOperation(contents[3]);
            data.setContentLen(strToLong(contents[4]));
            data.setTimeInNanos(strToLong(contents[5]));
            dataList.add(data);
          }
        }
      }
    }
    parsed = true;
  }

}
