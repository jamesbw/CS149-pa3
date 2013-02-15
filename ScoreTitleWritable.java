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
}