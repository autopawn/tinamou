extern crate timely;

mod hashjoin;

use hashjoin::HashJoin;
// use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::ToStream;

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
        let stream_1 = input_table_1.to_stream(scope);

        let mut input_table_2: Vec<Vec<String>> = Vec::new();
        let stream_2 = input_table_2.to_stream(scope);

        let jvars1 : Vec<usize> = Vec::new();
        let jvars2 : Vec<usize> = Vec::new();
        stream_1.hash_join(&stream_2,jvars1,jvars2);

    });
}
