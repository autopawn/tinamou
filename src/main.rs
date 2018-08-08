extern crate timely;
extern crate hyper;

mod hashjoin;

use timely::dataflow::operators::generic::source;
use std::io::{BufRead, BufReader};

use timely::dataflow::operators::Inspect;
use hashjoin::HashJoin; // Own hashjoin operator.

use hyper::Client;
use hyper::Url;
use hyper::client::response::Response;

fn response_to_table(res: Response) -> Vec<Vec<String>> {
    // Split request in lines, create a table entry for each line but the first one.
    let mut table: Vec<Vec<String>> = Vec::new();
    let mut first = true;
    for line in BufReader::new(res).lines() {
        if first {
            first = false;
            continue;
        }
        let linep = line.unwrap();
        let mut split = linep[1..linep.len()-1].split("\",\"");

        let mut entry : Vec<String> = Vec::new();
        for val in split {
            entry.push(val.to_owned());
        }
        table.push(entry);
    }
    // Return table
    table
}

fn main() {

    timely::example(|scope| {

        // Create a http client using hyper:
        let client = Client::new();

        // Create a query preset for both queries:

        let mut url = Url::parse("http://dbpedia.org/sparql").unwrap();
        url.query_pairs_mut().append_pair("default-graph-uri","http://dbpedia.org");
        url.query_pairs_mut().append_pair("format","text/csv");
        url.query_pairs_mut().append_pair("CXML_redir_for_subjs","121");
        url.query_pairs_mut().append_pair("CXML_redir_for_hrefs","");
        url.query_pairs_mut().append_pair("timeout","30000");
        url.query_pairs_mut().append_pair("debug","on");
        url.query_pairs_mut().append_pair("run","+Run+Query+");

        let query1 = "
        select distinct ?person ?name where {
            ?person rdf:type foaf:Person .
            ?person foaf:name ?name .
            FILTER(REGEX(STR(?name),\"^Ab+\"))
        } LIMIT 8000
        "; // Persons and their lastnames, as long as they start with "Ab".

        let query2 = "
        select distinct ?place ?name where {
            ?place rdf:type schema:Place .
            ?person foaf:name ?name .
            FILTER(REGEX(STR(?name),\"^Ab+\"))
        } LIMIT 8000
        "; // Places and their names, as long as they start with "Ab".


        // Define a source in the timely-dataflow graph for the first query.
        let stream_1 = source(scope, "tablequery1", |capability| {
            // Pass capability to an Option container so that it is free when its set to None.
            let mut capability = Some(capability);
            // Set request for query 1
            let mut request = url.clone();
            request.query_pairs_mut().append_pair("query",query1);
            // Perform request and get table:
            let res = client.get(request).send().unwrap();
            assert_eq!(res.status, hyper::Ok);
            let table = response_to_table(res);
            // Transform table into an iterator:
            let mut iterator = table.into_iter().fuse();
            // Closure that is called when this source node gets control:
            move |output| {
                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give(element);
                } else {
                    capability = None;
                }
            }
        });

        // Define a source in the timely-dataflow graph for the second query.
        let stream_2 = source(scope, "tablequery2", |capability| {
            // Pass capability to an Option container so that it is free when its set to None.
            let mut capability = Some(capability);
            // Set request of query 2
            let mut request = url.clone();
            request.query_pairs_mut().append_pair("query",query2);
            // Perform request and get table:
            let res = client.get(request).send().unwrap();
            assert_eq!(res.status, hyper::Ok);
            let table = response_to_table(res);
            // Transform table into an iterator:
            let mut iterator = table.into_iter().fuse();
            // Closure that is called when this source node gets control:
            move |output| {
                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give(element);
                } else {
                    capability = None;
                }
            }
        });

        // Perform a hash_join by the name columns, then inspect to print the resulting entries:
        stream_1.hash_join(&stream_2,vec![1],vec![1]).inspect(|x| println!("{:?}",x));

    });
}
