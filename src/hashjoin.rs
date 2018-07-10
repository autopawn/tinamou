//! Joins two input streams by given columns

use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::generic::operator::Operator;

use std::collections::HashMap;

/// Extension trait for hashjoin.
pub trait HashJoin<D: Data> {
    fn hash_join(&self, &Self, joinvars_a: Vec<usize>, joinvars_b: Vec<usize>) -> Self;
}

impl<G: Scope> HashJoin<Vec<String>> for Stream<G,Vec<String>> {
    fn hash_join(&self, stream_2 : &Self, joinvars_a: Vec<usize>, joinvars_b: Vec<usize>) -> Stream<G, Vec<String>> {
        self.binary(stream_2, Pipeline, Pipeline, "hash_join", {
            move |_capability, _info| {
                let mut table_a: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
                let mut table_b: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();

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
            }
        })
    }
}
