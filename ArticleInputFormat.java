public class ArticleInputFormat extends
    FileInputFormat<Text, Text> {

  public RecordReader<Text, Text> getRecordReader(
      InputSplit input, JobConf job, Reporter reporter)
      throws IOException {

    reporter.setStatus(input.toString());
    return new ArticleRecordReader(job, (FileSplit)input);
  }
}

class ArticleRecordReader implements RecordReader<Text, Text> {

  static private Pattern PATTERN = Pattern.compile("<title>(.*?)</title>(.*?)(<title>|$)");

  private FileSplit fileSplit;
  private Configuration conf;
  private Matcher matcher;
  private boolean started;
  private int fileLength;

  public ArticleRecordReader(JobConf job, FileSplit split) throws IOException {
    this.fileSplit = fileSplit;
    this.conf = conf;
    this.started = false;

    this.fileLength = (int) fileSplit.getLength();

    byte[] contents = new byte[(int) fileSplit.getLength()];
    Path file = fileSplit.getPath();

    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream in = null;
    try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);                
        fileAsString = new String(contents);
    } finally {
        IOUtils.closeStream(in);
    }

    this.matcher = Pattern.matcher(fileAsString);
  }

  public boolean next(Text key, Text value) throws IOException {
    if (matcher.find()) {
      key.set(matcher.start(1));
      value.set(matcher.start(2));
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