James Whitbeck
jamesbw
CS149 pa3

Input parsing
-------------
I created a custom InputFormat called ArticleInputFormat. It parses the file split to find titles and content for each page. It outputs a pair consisting of the page title and the page content as Text.

Generating Ngrams
-----------------
A NGramBag corresponds to each page. It contains an associative map of ngrams to the number of times it appears in the page. When scoring a page for a query, we sum up the occurrences of each ngram from the query in the page.

Mapper and Reducer
------------------
The mapper takes each title-content pair, and matches the content against the query ngrams. It outputs all title-score pairs to the same key.
The reducer, operating on this single key, outputs the 20 title-score pairs with the highest score.
A combiner takes the output of each mapper task and limits the values to 20 per mapper, so that the reducer doesn't get overwhelmed.

Running time
------------
Each mapper generates NGrams from the query: O(q). Then number of ngrams produced from a whole filesplit is O(n/P). The matching of ngrams with the query takes constant time because we use a hashmap. So each mapper takes time O(q + n/P).
The reducer receives at most 20 pairs from each mapper. The reducer maintains a priority queue of size 20, and iterates through all the pairs, inserting them into the priority queue only if they have scores greater than the minimum score of the priority queue. Since the queue is of fixed length, insertion takes constant time. So the reducer runs in time linear with the number of pairs. If the combiners were run in a tree-like fashion, we could expect O(log(P)) running time, but the Hadoop implementation we are dealing with will most likely do O(P).

DistributedCache
----------------
The query file is distributed to the mappers using DistributedCache.

