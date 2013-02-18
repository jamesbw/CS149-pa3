
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class NGram extends Configured implements Tool {

    // public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
    public static class Map extends MapReduceBase implements Mapper<Text, Text, IntWritable, ScoreTitleWritable> {

      // static enum Counters { INPUT_WORDS }

      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      private boolean caseSensitive = true;
      // private Set<String> patternsToSkip = new HashSet<String>();

      private long numRecords = 0;
      private int ngramSize;
      private String inputFile;
      private static NGramBag queryBag;

      public void configure(JobConf job) {
        caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
        inputFile = job.get("map.input.file");

        ngramSize = job.getInt("ngram.size", 3);

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


        // if (job.getBoolean("wordcount.skip.patterns", false)) {
        //   Path[] patternsFiles = new Path[0];
        //   try {
        //     patternsFiles = DistributedCache.getLocalCacheFiles(job);
        //   } catch (IOException ioe) {
        //     System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
        //   }
        //   for (Path patternsFile : patternsFiles) {
        //     parseSkipFile(patternsFile);
        //   }
        // }
      }

      // private void parseSkipFile(Path patternsFile) {
      //   try {
      //     BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
      //     String pattern = null;
      //     while ((pattern = fis.readLine()) != null) {
      //       patternsToSkip.add(pattern);
      //     }
      //   } catch (IOException ioe) {
      //     System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
      //   }
      // }

      // public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      public void map(Text key, Text value, OutputCollector<IntWritable, ScoreTitleWritable> output, Reporter reporter) throws IOException {

      	NGramBag bag = new NGramBag(value.toString(), ngramSize);

      	// int similarity = bag.similarity(queryBag);
        int similarity = bag.score(queryBag);
      	if (similarity > 0) {
        	System.out.println(key);
          // output.collect(key, new IntWritable(similarity));
          output.collect(one, new ScoreTitleWritable(similarity, key.toString()));
        }

        // String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

        // for (String pattern : patternsToSkip) {
        //   line = line.replaceAll(pattern, "");
        // }

        // StringTokenizer tokenizer = new StringTokenizer(line);
        // while (tokenizer.hasMoreTokens()) {
        //   word.set(tokenizer.nextToken());
        //   output.collect(word, one);
		//   reporter.incrCounter(Counters.INPUT_WORDS, 1);
        // }

        if ((++numRecords % 10) == 0) {
          reporter.setStatus("Finished processing " + numRecords + " articles " + "from the input file: " + inputFile);
        }
      }
    }

    // public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, ScoreTitleWritable, Text, IntWritable> {
      // public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
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
            if (ScoreTitleWritable.comparator.compare(article, queue.peek()) >= 0) {
              queue.add(new ScoreTitleWritable(article.getScore(), article.getTitle()));
              queue.poll();
            }
          }
        }

        while (!queue.isEmpty()){
          ScoreTitleWritable article = queue.poll();
          output.collect(new Text(article.getTitle()), new IntWritable(article.getScore()));
        }

        // List<ScoreTitleWritable> list = new LinkedList<ScoreTitleWritable>();
        // while (values.hasNext()) {
        //   ScoreTitleWritable article = values.next();
        //   System.out.println(article);
        //   list.add(new ScoreTitleWritable(article.getScore(), article.getTitle()));
        // }

        // System.out.println("Values: " + list.size());

        // Collections.sort(list, new Comparator(){
        //   @Override
        //   public int compare(Object o1, Object o2){
        //     int score1 = ((ScoreTitleWritable) o1).getScore();
        //     String title1 = ((ScoreTitleWritable) o1).getTitle();
        //     int score2 = ((ScoreTitleWritable) o2).getScore();
        //     String title2 = ((ScoreTitleWritable) o2).getTitle();

        //     if (score1 < score2) return 1;
        //     if (score1 > score2) return -1;
        //     return title2.compareTo(title1);
        //   }
        // });


        // for (int i = 0; i < NUM_OUTPUT ; i++) {
        //   ScoreTitleWritable article = list.get(i);
        //   System.out.println("Title: " + article.getTitle() + " , Score: " + article.getScore());
        //   output.collect(new Text(article.getTitle()), new IntWritable(article.getScore()));
        // }


      	// output.collect(key, values.next());
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
      // conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      // conf.setInputFormat(ArticleInputFormat.class);
      conf.setInputFormat(ArticleInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);
      conf.setInt("ngram.size", Integer.parseInt(args[0]));

      // List<String> other_args = new ArrayList<String>();
      // for (int i=0; i < args.length; ++i) {
      //   if ("-skip".equals(args[i])) {
      //     DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
      //     conf.setBoolean("wordcount.skip.patterns", true);
      //   } else {
      //     other_args.add(args[i]);
      //   }
      // }

      // FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
      // FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

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