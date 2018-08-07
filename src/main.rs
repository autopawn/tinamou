extern crate timely;
extern crate hyper;

mod hashjoin;

use std::io::{BufRead, BufReader};

use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::ToStream;
use hashjoin::HashJoin; // Own hashjoin operator.

use hyper::Client;
use hyper::Url;

fn map2str(strs : Vec<&str>) -> Vec<String> {
    // Creates a vector of String from a vector of references to constant strs.
    let mut st : Vec<String> = Vec::new();
    for val in strs {
        st.push(String::from(val));
    }
    return st;
}

fn main() {

    timely::example(|scope| {

        let query1 = "
        select distinct ?person ?name ?city where {
          ?person rdf:type foaf:Person .
          ?person foaf:name ?name .
          ?city rdf:type schema:City .
          ?city foaf:name ?name
        } LIMIT 1000
        "; // Persons that have the same name than a city.

        let query2 = "
        select distinct ?city ?hospital where {
          ?city rdf:type schema:City .
          ?hospital rdf:type schema:Hospital .
          ?hospital dbp:location ?city .
        } LIMIT 1000
        "; // Hospitals and their cities.

        let mut url = Url::parse("http://dbpedia.org/sparql").unwrap();
        url.query_pairs_mut().append_pair("default-graph-uri","http://dbpedia.org");
        url.query_pairs_mut().append_pair("format","text/csv");
        url.query_pairs_mut().append_pair("CXML_redir_for_subjs","121");
        url.query_pairs_mut().append_pair("CXML_redir_for_hrefs","");
        url.query_pairs_mut().append_pair("timeout","30000");
        url.query_pairs_mut().append_pair("debug","on");
        url.query_pairs_mut().append_pair("run","+Run+Query+");
        url.query_pairs_mut().append_pair("query",query1);

        // Create a http client using hyper:
        let client = Client::new();

        let res = client.get(url).send().unwrap();
        assert_eq!(res.status, hyper::Ok);

        for line in BufReader::new(res).lines() {
            println!("{}", line.unwrap());
        }



        // let stream_1 = source(scope, "tables", move |capability| {
        //     let mut table: Vec<Vec<String>> = Vec::new();
        //
        //     let mut iterator = table.into_iter().fuse();
        //     let mut capability = Some(capability);
        //
        //     move |output| {
        //         if let Some(element) = iterator.next() {
        //             let mut session = output.session(capability.as_ref().unwrap());
        //
        //             session.give(element);
        //         } else {
        //             capability = None;
        //         }
        //     }
        // });

        // let stream_2 = source(scope, "tables", move |capability| {
        //     let mut table: Vec<Vec<String>> = Vec::new();
        //
        //     let mut iterator = table.into_iter().fuse();
        //     let mut capability = Some(capability);
        //
        //     move |output| {
        //         if let Some(element) = iterator.next() {
        //             let mut session = output.session(capability.as_ref().unwrap());
        //
        //             session.give(element);
        //         } else {
        //             capability = None;
        //         }
        //     }
        // });

        let mut input_table_1: Vec<Vec<String>> = Vec::new();
        input_table_1.push(map2str(vec!["Ab1","100","200","Hi1"]));
        input_table_1.push(map2str(vec!["Ab2","100","200","Hi2"]));
        input_table_1.push(map2str(vec!["Ab3","200","500","Hi3"]));
        input_table_1.push(map2str(vec!["Ab4","200","800","Hi4"]));
        let stream_1 = input_table_1.to_stream(scope);

        let mut input_table_2: Vec<Vec<String>> = Vec::new();
        input_table_2.push(map2str(vec!["200","800","Bye1"]));
        input_table_2.push(map2str(vec!["100","200","Bye2"]));
        input_table_2.push(map2str(vec!["100","400","Bye3"]));
        input_table_2.push(map2str(vec!["Ab3","200","500","Hi3"]));
        let stream_2 = input_table_2.to_stream(scope);

        stream_1.hash_join(&stream_2,vec![1,2],vec![0,1]).inspect(|x| println!("{:?}",x));

    });
}
