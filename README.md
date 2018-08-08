# Tinamou

An example of a timely-dataflow operator to perform hash joins on tables
originated from SPARQL queries to dbpedia.

# Conclusions

## Usage of the timely-dataflow library

Declaring operators for timely-dataflow (and sources, which are a special case
of operators with no inputs) can be done using the
`source`, `unary`, and `binary` functions which receive closures
(similar to lambda expresions), as [indicated in the mdbook](http://www.frankmcsherry.org/timely-dataflow/chapter_2/chapter_2_4.html).

The most effective method found to define operators was to mimic the
[examples](https://github.com/frankmcsherry/timely-dataflow/tree/master/examples)
in the timely-dataflow repository, by creating extension traits, as was done in [hashjoin.rs](./src/hashjoin.rs), which was based in the example with the same name.

The usage of closures seems to be common as they provide type-inference and the
library uses complex, auto-generated datatypes, however this practice
doesn't allow proper code reuse.

## Streaming responses

Entries can be easily parsed from a response to an HTTP GET request in CSV format,
in this example all the entries are received synchronically. Real applications,
however, should work with [streaming responses](https://github.com/weblyzard/streaming-sparql) in order to avoid the bottleneck
of waiting for the whole table before the sources send their entries
to the dataflow graph, otherwise the benefits of an streaming system are lost.

Other option is partitioning the entries manually by performing several queries on the
graph sources using the [**limit**, **order by**, and **offset**](https://stackoverflow.com/a/27488839)
keywords.

## Timely-dataflow

The main premise of timely-dataflow is the capability of performing iterative
and incremental computations trough cyclic dataflow programs without overhead
(not having to support the modification of the graph in runtime), if this
feature is not necessary, other streaming systems can be used. This seems to be
the case for operators on a query execution plan.

## On rust ecosystem

Used libraries proven to be constantly evolving, so most of the documentation
and tutorials are inaccurate. This also happens to the [timely-dataflow mdbook](http://www.frankmcsherry.org/timely-dataflow/), the usage of an IDE is
highly recommended, together with use of the official documentation hosted in
[https://docs.rs/](https://docs.rs/), however, a good understanding
of the obscure features of the language is required.
