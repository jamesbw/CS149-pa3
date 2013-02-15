import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;

public class ScoreTitleWritable implements Writable {
  // Some data     
  private int score;
  private String title;

  public ScoreTitleWritable(){}

  public ScoreTitleWritable(int score, String title){
    this.score = score;
    this.title = title;
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(score);
    out.writeUTF(title);
  }
  
  public void readFields(DataInput in) throws IOException {
    score = in.readInt();
    title = in.readUTF();
  }
  
  public static ScoreTitleWritable read(DataInput in) throws IOException {
    ScoreTitleWritable w = new ScoreTitleWritable();
    w.readFields(in);
    return w;
  }

  public int getScore(){
    return score;
  }

  public String getTitle(){
    return title;
  }

  @Override
  public String toString(){
    return "Title: " + title + ", Score: " + score;
  }

  public static Comparator comparator = new Comparator(){
    @Override
    public int compare(Object o1, Object o2){
      int score1 = ((ScoreTitleWritable) o1).getScore();
      String title1 = ((ScoreTitleWritable) o1).getTitle();
      int score2 = ((ScoreTitleWritable) o2).getScore();
      String title2 = ((ScoreTitleWritable) o2).getTitle();

      if (score1 > score2) return 1;
      if (score1 < score2) return -1;
      return title1.compareTo(title2);
    }
  };
}