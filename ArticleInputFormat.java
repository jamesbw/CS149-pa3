import java.io.IOException;

import org.apache.hadoop.io.Text;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.conf.Configuration;
 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ArticleInputFormat extends
    FileInputFormat<Text, Text> {

  @Override
  public RecordReader<Text, Text> getRecordReader(
      InputSplit input, JobConf job, Reporter reporter)
      throws IOException {

    reporter.setStatus(input.toString());
    return new ArticleRecordReader(job, (FileSplit)input);
  }
}

class ArticleRecordReader implements RecordReader<Text, Text> {

  static private Pattern PATTERN = Pattern.compile("<title>(.*?)</title>(.*?)(<title>|$)");

  // private FileSplit fileSplit;
  // private Configuration conf;
  private Matcher matcher;
  private boolean started;
  private int fileLength;

  public ArticleRecordReader(JobConf job, FileSplit split) throws IOException {
    // this.fileSplit = fileSplit;
    // this.conf = conf;
    this.started = false;

    this.fileLength = (int) fileSplit.getLength();

    byte[] contents = new byte[(int) fileSplit.getLength()];
    Path file = fileSplit.getPath();

    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream in = null;
    String fileAsString;
    try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);                
        fileAsString = new String(contents);
    } finally {
        IOUtils.closeStream(in);
    }

    this.matcher = PATTERN.matcher(fileAsString);
  }

  public boolean next(Text key, Text value) throws IOException {
    if (matcher.find()) {
      key.set(matcher.group(1));
      value.set(matcher.group(2));
      started = true;
      return true;
    }
    else
      return false;
  }

  public Text createKey() {
    return new Text("");
  }

  public Text createValue() {
    return new Text("");
  }

  public long getPos() throws IOException {
    return started ? matcher.start() : 0;
  }

  public void close() throws IOException {
  }

  public float getProgress() throws IOException {
    return started ? matcher.start() / fileLength : 0;
  }
}