package com.yahoo.ycsb.generator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

/**
 * Generates long values read from a File.
 * <p>
 * Eventually nextValue() restarts at the beginning of the sequence.
 */
public class FileLongGenerator extends NumberGenerator {
  private List<String> list;
  private int current = 0;

  /**
   * Create a FileLongGenerator with the given file.
   *
   * @param filename The file to read lines from.
   */
  public FileLongGenerator(String filename) {
    try {
      list = Files.readAllLines(new File(filename).toPath(), Charset.defaultCharset());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public Long nextValue() {
    long value = Long.valueOf(list.get(current));
    if (current + 2 > list.size()) {
      current = 0;
    } else {
      current++;
    }
    return value;
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
  }
}
