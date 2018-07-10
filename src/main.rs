extern crate timely;

// use timely::dataflow::operators::*;
// use timely::dataflow::*;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::ToStream;

use timely::dataflow::InputHandle;

use std::collections::HashMap;

fn main() {
    println!("Hello, world!");

    timely::example(|scope| {

        let stream_1 = source(scope, "tables", move |capability| {
            let mut table: Vec<Vec<String>> = Vec::new();

            let mut iterator = table.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {
                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());

                    session.give(element);
                } else {
                    capability = None;
                }
            }
        });

        let stream_2 = source(scope, "tables", move |capability| {
            let mut table: Vec<Vec<String>> = Vec::new();

            let mut iterator = table.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {
                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());

                    session.give(element);
                } else {
                    capability = None;
                }
            }
        });

        stream_1.binary(&stream_2, Pipeline, Pipeline, "increment", move |_capability, _info| {
            let mut table_a: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
            let mut table_b: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
            let joinvars_a: Vec<usize> = Vec::new();
            let joinvars_b: Vec<usize> = Vec::new();

            move |input_1, input_2, output| {

                while let Some((time, data)) = input_1.next() {
                    let mut session = output.session(&time);
                    // Threat everything from input1 in table_a:
                    for entry in data.drain(..) {
                        let mut key: Vec<String> = Vec::new();
                        let mut value: Vec<String> = Vec::new();
                        for i in 0..entry.len() {
                            if joinvars_a.contains(&i) {
                                key.push(entry[i].clone());
                            } else {
                                value.push(entry[i].clone());
                            }
                        }
                        // Put input on table_a:
                        if !table_a.contains_key(&key) {
                            table_a.insert(key.clone(),Vec::new());
                        }
                        table_a.get_mut(&key).unwrap().push(value.clone());
                        // Cross with table_b:
                        if table_b.contains_key(&key) {
                            for value_b in table_b.get(&key).unwrap() {
                                let mut nentry = Vec::new();
                                nentry.append(&mut value.clone());
                                nentry.append(&mut key.clone());
                                nentry.append(&mut value_b.clone());
                                session.give(nentry);
                            }
                        }
                    }
                }

                while let Some((time, data)) = input_2.next() {
                    let mut session = output.session(&time);
                    // Threat everything from input2 in table_b:
                    for entry in data.drain(..) {
                        let mut key: Vec<String> = Vec::new();
                        let mut value: Vec<String> = Vec::new();
                        for i in 0..entry.len() {
                            if joinvars_b.contains(&i) {
                                key.push(entry[i].clone());
                            } else {
                                value.push(entry[i].clone());
                            }
                        }
                        // Put input on table_b:
                        if !table_b.contains_key(&key) {
                            table_b.insert(key.clone(),Vec::new());
                        }
                        table_b.get_mut(&key).unwrap().push(value.clone());
                        // Cross with table_a:
                        if table_a.contains_key(&key) {
                            for value_a in table_a.get(&key).unwrap() {
                                let mut nentry = Vec::new();
                                nentry.append(&mut value_a.clone());
                                nentry.append(&mut key.clone());
                                nentry.append(&mut value.clone());
                                session.give(nentry);
                            }
                        }
                    }
                }
            }
        });
    });
}
