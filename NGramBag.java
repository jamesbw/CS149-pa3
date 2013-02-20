import java.util.*;
import java.io.*;

public class NGramBag implements Serializable {

	public Map<NGramInstance, Integer> ngramCounts;

	public NGramBag(String text, int size) {
		this.ngramCounts = new HashMap<NGramInstance, Integer>();
		LinkedList<String> words = new LinkedList<String>();
		Tokenizer tokenizer = new Tokenizer(text);
		while (tokenizer.hasNext()) {
			words.add(tokenizer.next());
			if(words.size() > size) {
				words.remove();
				NGramInstance newInstance = new NGramInstance(words);
				if (!this.ngramCounts.containsKey(newInstance)) {
					this.ngramCounts.put(newInstance, 1);
				}
				else {
					this.ngramCounts.put(newInstance, this.ngramCounts.get(newInstance) + 1);
				}
			}
		}
	}
	public int score(NGramBag query) {
		int score = 0;
		for (NGramInstance ngram : query.ngramCounts.keySet()) {
			if (ngramCounts.containsKey(ngram)) {
				score += ngramCounts.get(ngram);
			}
		}
		return score;
	}

	public String toString(){
		return ngramCounts.toString();
	}
}

class NGramInstance implements Serializable {
	public List<String> words;

	public NGramInstance(List words) {
		this.words = new LinkedList(words);
	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass().equals(NGramInstance.class)) {
			return ((NGramInstance) o).words.equals(words);
		}
		else
			return false;
	}

	@Override
	public int hashCode() {
		return words.hashCode();
	}

	public String toString(){
		return words.toString();
	}
}