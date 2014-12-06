Extracting Anchor Text
---------------------

To extract anchor text from the ClueWeb09 collection, please use the following:

	hadoop jar cloud9.jar edu.umd.cloud9.webgraph.driver.ClueWebDriver
	-input collection-base-path -output output-base-path -docno docno-mapping-file
	-begin frist-segment-number -end last-segment-number -normalizer normalizer-class
	[-il] [-caw]

* `-begin` and `-end`: For example, to extract anchors from segments 2, 3, and 4 use `-begin 2 -end 4` and similarly use `-begin n -end n` to extract anchors from segment `n`.
* `-il`: Consider internal links. Without this option internal links will be discarded.
* `-caw`: Compute the default weighting scheme proposed by [Metzler et. al.](http://dl.acm.org/citation.cfm?id=1571981).
* `-normalizer`: A subclass of `edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer`.

Note that the input must be a collection of sequence files.

For other TREC collections, use the following driver instead:

	hadoop jar cloud9.jar edu.umd.cloud9.webgraph.driver.TrecDriver
	-input collection-base-path -output output-base-path [-collection gov2|wt10g|trecweb] -docno docno-mapping-file
	-normalizer normalizer-class [-il] [-caw] [-inputFormat input-format-class] [-docnoClass docno-mapping-class]

* `-inputFormat` and `-docnoClass`: When `-collection` is not specified, you must provide the input format and docno mapping class in order to run this generic driver on collections other than the supported set (i.e., gov2, wt10g, and trecweb). Please note that to be compatible with the framework, the input document set must be a collection of `WebDocument`s.

Note that when `-collection` is used, `-input` must point to the *raw* document collection (not repacked sequence files).

Building Indexable Anchor Collections
---------------------------------

To be able to index an anchor collection, anchors must be Indexable. The following driver converts the output of the above into Indexable objects.

	hadoop jar cloud9.jar edu.umd.cloud9.webgraph.driver.BuildIndexableAnchorCollection
	-input anchor-collection-path -output output-path -docnoClass docno-mapping-class -docno docno-mapping-path
	-numReducers number-of-reducers [-maxLength maximum-content-length]


