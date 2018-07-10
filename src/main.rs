extern crate timely;

mod hashjoin;

use timely::dataflow::operators::Inspect;
use hashjoin::HashJoin;
// use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::ToStream;

fn map2str(strs : Vec<&str>) -> Vec<String> {
    let mut st : Vec<String> = Vec::new();
    for val in strs {
        st.push(String::from(val));
    }
    return st;
}

fn main() {
    println!("Hello, world!");

    timely::example(|scope| {

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
