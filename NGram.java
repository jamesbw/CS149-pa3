
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class NGram extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<Text, Text, IntWritable, ScoreTitleWritable> {

      private final static IntWritable one = new IntWritable(1);

      private long numRecords = 0;
      private int ngramSize;
      private String inputFile;
      private static NGramBag queryBag;

      public void configure(JobConf job) {
        inputFile = job.get("map.input.file");
        ngramSize = job.getInt("ngram.size", 4);

        String queryString = "";

        if (queryBag == null){
          try {
  	        Path queryFile = DistributedCache.getLocalCacheFiles(job)[0];

  	        BufferedReader fis = new BufferedReader(new FileReader(queryFile.toString()));
  	        String queryLine = null;
  	        while( (queryLine = fis.readLine()) != null) {
  	        	queryString += "\n" + queryLine;
  	        }
          }
          catch (Exception e){
          	e.printStackTrace();
          }

          queryBag = new NGramBag(queryString, ngramSize);
        }
      }

      public void map(Text key, Text value, OutputCollector<IntWritable, ScoreTitleWritable> output, Reporter reporter) throws IOException {

      	NGramBag bag = new NGramBag(value.toString(), ngramSize);

        int similarity = bag.score(queryBag);
      	if (similarity > 0) {
        	System.out.println(key);
          output.collect(one, new ScoreTitleWritable(similarity, key.toString()));
        }

        if ((++numRecords % 10) == 0) {
          reporter.setStatus("Finished processing " + numRecords + " articles " + "from the input file: " + inputFile);
        }
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, ScoreTitleWritable, Text, IntWritable> {
      public void reduce(IntWritable key, Iterator<ScoreTitleWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        int NUM_OUTPUT = 20;

        java.util.PriorityQueue<ScoreTitleWritable> queue = new java.util.PriorityQueue<ScoreTitleWritable>(NUM_OUTPUT + 1, ScoreTitleWritable.comparator);

        while (values.hasNext()) {
          ScoreTitleWritable article = values.next();
          System.out.println(article);
          if (queue.size() < NUM_OUTPUT){
            queue.add(new ScoreTitleWritable(article.getScore(), article.getTitle()));
          }
          else {
            if (ScoreTitleWritable.comparator.compare(article, queue.peek()) > 0) {
              queue.add(new ScoreTitleWritable(article.getScore(), article.getTitle()));
              queue.poll();
            }
          }
        }

        while (!queue.isEmpty()){
          ScoreTitleWritable article = queue.poll();
          output.collect(new Text(article.getTitle()), new IntWritable(article.getScore()));
        }
      }
    }

    public static class Combine extends MapReduceBase implements Reducer<IntWritable, ScoreTitleWritable, IntWritable, ScoreTitleWritable> {
      private final static IntWritable one = new IntWritable(1);
      public void reduce(IntWritable key, Iterator<ScoreTitleWritable> values, OutputCollector<IntWritable, ScoreTitleWritable> output, Reporter reporter) throws IOException {

        int NUM_OUTPUT = 20;

        java.util.PriorityQueue<ScoreTitleWritable> queue = new java.util.PriorityQueue<ScoreTitleWritable>(NUM_OUTPUT + 1, ScoreTitleWritable.comparator);

        while (values.hasNext()) {
          ScoreTitleWritable article = values.next();
          System.out.println(article);
          if (queue.size() < NUM_OUTPUT){
            queue.add(new ScoreTitleWritable(article.getScore(), article.getTitle()));
          }
          else {
            if (ScoreTitleWritable.comparator.compare(article, queue.peek()) >= 0) {
              queue.add(new ScoreTitleWritable(article.getScore(), article.getTitle()));
              queue.poll();
            }
          }
        }

        while (!queue.isEmpty()){
          ScoreTitleWritable article = queue.poll();
          output.collect(one, article);
        }
      }
    }

    public int run(String[] args) throws Exception {
      JobConf conf = new JobConf(getConf(), NGram.class);
      conf.setJobName("ngram");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);

      conf.setMapOutputKeyClass(IntWritable.class);
      conf.setMapOutputValueClass(ScoreTitleWritable.class);

      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Combine.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(ArticleInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);
      conf.setInt("ngram.size", Integer.parseInt(args[0]));

      FileInputFormat.setInputPaths(conf, new Path(args[2]));
      FileOutputFormat.setOutputPath(conf, new Path(args[3]));

      JobClient.runJob(conf);
      return 0;
    }

    public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new NGram(), args);
      System.exit(res);
    }
}