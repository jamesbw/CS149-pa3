import java.util.*;


public class NGramBag {

	private Set<NGramInstance> bag;

	public NGramBag(String text, int size) {
		this.bag = new HashSet<NGramInstance>();
		LinkedList<String> words = new LinkedList<String>();
		Tokenizer tokenizer = new Tokenizer(text);
		while (tokenizer.hasNext()) {
			words.add(tokenizer.next());
			if(words.size() > size) {
				words.remove();
				bag.add(new NGramInstance(words));
			}
		}
	}

	public boolean contains(NGramInstance ngram) {
		return bag.contains(ngram);
	}

	public int similarity(NGramBag other) {
		int score = 0;
		for (NGramInstance ngram : bag) {
			if (other.contains(ngram)) {
				score ++;
			}
		}
		return score;
	}
}

class NGramInstance {
	public List<String> words;

	public NGramInstance(words) {
		this.words = new LinkedList(words);
	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass().equals(NGramInstance.class)) {
			return o.words.equals(words);
		}
		else
			return false;
	}

	@Override
	public int hashCode() {
		return words.hashCode();
	}
}