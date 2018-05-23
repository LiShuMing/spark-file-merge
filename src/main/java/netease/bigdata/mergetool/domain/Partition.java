package netease.bigdata.mergetool.domain;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

/**
 * A record of moving mergedfiles in tmp dirs to original dirs
 */
public class Partition {
  private String dir;
  private int numMerged;
  private String inputPaths;
  private static final String INPUT_PATH_SEPARATOR = "|::|";

  public Partition(){
  }

  public Partition(String dir, int numMerged, String inputPaths) {
    this.dir = dir;
    this.numMerged = numMerged;
    this.inputPaths = inputPaths;
  }

  public String getDir() {
    return dir;
  }

  public void setDir(String dir) {
    this.dir = dir;
  }

  public int getNumMerged() {
    return numMerged;
  }

  public void setNumMerged(int numMerged) {
    this.numMerged = numMerged;
  }

  public String getInputPaths() {
    return inputPaths;
  }

  public void setInputPaths(String inputPaths) {
    this.inputPaths = inputPaths;
  }

  public static String[] decodeInputPaths(String whole) {
    return whole.split(Pattern.quote(INPUT_PATH_SEPARATOR));
  }

  public static String encodeInputPaths(String[] parts) {
    Preconditions.checkArgument(parts.length > 0);
    StringBuilder sb = new StringBuilder(parts[0]);
    for (int i = 1; i < parts.length; i++) {
      sb.append(INPUT_PATH_SEPARATOR).append(parts[i]);
    }
    return sb.toString();
  }
}
