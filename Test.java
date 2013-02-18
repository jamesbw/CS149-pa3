import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.util.*;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;

public class Test {
	// from stackoverflow
	private static String readFile(String path) throws IOException {
	  FileInputStream stream = new FileInputStream(new File(path));
	  try {
	    FileChannel fc = stream.getChannel();
	    MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
	    /* Instead of using default, pass in a decoder. */
	    return Charset.defaultCharset().decode(bb).toString();
	  }
	  finally {
	    stream.close();
	  }
	}

	private static void testPattern() throws IOException {
		Pattern PATTERN = Pattern.compile("<title>(.*?)</title>(.*?)(?=(<title>|$))", Pattern.DOTALL);
		// Pattern PATTERN = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
		String file = readFile("./chunk_aa");
		Matcher matcher = PATTERN.matcher(file);
		int count = 0;

		while(matcher.find() && count < 5){
			System.out.println("Title: " + matcher.group(1));
			System.out.println("Text: " + matcher.group(2));
			count ++;
		}
	}

	private static void testBag() throws IOException {
		String queryFile = readFile("./query1.txt");
		NGramBag bag = new NGramBag(queryFile, 4);
		System.out.println(bag);
		System.out.println(bag.score(bag));
	}

	private static void testScore() throws IOException {
		String queryText = readFile("./query1.txt");
		NGramBag queryBag = new NGramBag(queryText, 4);

		String wikiFile = readFile("./chunk_aa");
		Pattern PATTERN = Pattern.compile("<title>AVL tree</title>(.*?)(?=(<title>|$))", Pattern.DOTALL);
		Matcher matcher = PATTERN.matcher(wikiFile);

		matcher.find();
		String astroText = matcher.group(1);
		NGramBag astroBag = new NGramBag(astroText, 4);
		System.out.println(astroBag);
		System.out.println(astroBag.score(queryBag));
		System.out.println(queryBag.score(astroBag));
	}

	public static void main(String[] args) throws IOException{
		// testPattern();
		// testBag();
		testScore();
	}
}