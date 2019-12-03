use crate::transaction::{Op, Transaction, Key, Value};
use std::collections::{HashMap, HashSet};

pub struct SerChecker<K: Key, V: Value> {
    pub transactions: Vec<Vec<Transaction<K, V>>>,

    pub searched: Vec<usize>,
    pub searched_cache: HashMap<Vec<usize>, bool>,

    pub kv_rev: HashMap<(K, V), HashSet<(usize, usize)>>,
}

impl<K: Key, V: Value> SerChecker<K, V> {
    pub fn new(transactions: Vec<Vec<Transaction<K, V>>>) -> Self {
        let searched = vec![0; transactions.len()];

        let mut kv_rev: HashMap<(K, V), HashSet<(usize, usize)>> = HashMap::new();
        for (c, client) in transactions.iter().enumerate() {
            for (d, t) in client.iter().enumerate() {
                for op in t.ops.iter() {
                    if let Op::Set(set) = op {
                        match kv_rev.get_mut(&(set.key.clone(), set.val.clone())) {
                            Some(records) => {
                                records.insert((c, d));
                            }
                            None => {
                                let mut read_froms = HashSet::new();
                                read_froms.insert((c, d));
                                match kv_rev.insert((set.key.clone(), set.val.clone()), read_froms ) {
                                    Some(_) => {unreachable!()}
                                    None => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        Self {
            searched,
            transactions,
            kv_rev,
            searched_cache: HashMap::new(),
        }
    }

    fn target_len(&self) -> usize {
        self.transactions.iter().map(|t| t.len()).sum()
    }

    fn searched_len(&self) -> usize {
        self.searched.iter().sum()
    }

    pub fn check(&mut self) -> bool {
        dbg!(&self.searched);
        if self.searched_len() == self.target_len() {
            return true;
        }

        'a: for index in 0..self.transactions.len() {
            if self.searched[index] < self.transactions[index].len() {
                let considering_transaction = &self.transactions[index][self.searched[index]];

                for op in considering_transaction.ops.iter() {
                    if let Op::Get(get) = op {
                        let read_froms = self
                            .kv_rev
                            .get(&(get.key.clone(), get.val.clone()))
                            .unwrap();

                        if read_froms.iter().map(|(c, d)| d >= &self.searched[*c]).fold(true, |acc, x| acc && x) {
                            continue 'a;
                        }
                    }
                }

                for client_index in 0..self.transactions.len() {
                    let mut bottom = self.searched[client_index];
                    if client_index == index {
                        bottom += 1; // exclude the judging transaction
                    }

                    for index_ in bottom..self.transactions[client_index].len() {
                        let t = &self.transactions[client_index][index_];

                        for op in t.ops.iter() {
                            if let Op::Get(get) = op {
                                let key = get.key.clone();
                                let val = get.val.clone();

                                if considering_transaction.writes(key.clone()) {
                                    let read_froms = self
                                        .kv_rev
                                        .get(&(get.key.clone(), get.val.clone()))
                                        .unwrap();
                                    if read_froms.iter().map(|(c, d)| d < &self.searched[*c]).fold(true, |acc, x| acc && x) {
                                        // outside cannot read from inside of history if the searching transaction also writes key
                                        continue 'a;
                                    }
                                }
                            }
                        }
                    }
                }

                self.searched[index] += 1;
                match self.searched_cache.get(&self.searched) {
                    Some(value) => {
                        if *value {
                            return true;
                        } else {
                            self.searched[index] -= 1;
                        }
                    }
                    None => {
                        if self.check() {
                            self.searched_cache.insert(self.searched.clone(), true);

                            return true;
                        } else {
                            self.searched_cache.insert(self.searched.clone(), false);
                            self.searched[index] -= 1;
                        }
                    }
                }
            }
        }

        false
    }
}
