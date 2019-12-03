use crate::ser_checker::SerChecker;
use std::collections::{HashSet, HashMap};
use std::hash::Hash;
use std::fmt::Debug;

pub trait GenerateGuard {
    fn generate_guard(&self, index: usize) -> Self;
}

pub trait AbnormalValue {
    fn abnormal_value() -> Self;
}

impl GenerateGuard for usize {
    fn generate_guard(&self, index: usize) -> Self {
        index << 10 + *self
    }
}

impl AbnormalValue for usize {
    fn abnormal_value() -> Self {
        1
    }
}

impl GenerateGuard for String {
    fn generate_guard(&self, index: usize) -> Self {
        format!("__checker__{}__{}", index, self)
    }
}

impl AbnormalValue for String {
    fn abnormal_value() -> Self {
        "1".to_string()
    }
}

pub trait Key: Clone + Eq + Hash + GenerateGuard + Debug {}
pub trait Value: Clone + Eq + Hash + Default + AbnormalValue + Debug {}

impl<T: Clone + Eq + Hash + GenerateGuard + Debug> Key for T {}
impl<T: Clone + Eq + Hash + Default + AbnormalValue + Debug> Value for T {}

#[derive(Clone, Debug)]
pub struct Set<K: Key, V: Value> {
    pub key: K,
    pub val: V,
}

impl<K: Key, V: Value> Set<K, V> {
    pub fn new(key: K, val: V) -> Self {
        Set { key, val }
    }
}

#[derive(Clone, Debug)]
pub struct Get<K: Key, V: Value> {
    pub key: K,
    pub val: V,
}

impl<K: Key, V: Value> Get<K, V> {
    pub fn new(key: K, val: V) -> Self {
        Get { key, val }
    }
}

#[derive(Clone, Debug)]
pub enum Op<K: Key, V: Value> {
    Set(Set<K, V>),
    Get(Get<K, V>),
}

#[derive(Clone, Debug)]
pub struct Transaction<K: Key, V: Value> {
    pub ops: Vec<Op<K, V>>,
}

impl<K: Key, V: Value> Transaction<K, V> {
    pub fn writes(&self, key: K) -> bool {
        for op in self.ops.iter() {
            if let Op::Set(set) = op {
                if set.key == key {
                    return true;
                }
            }
        }

        false
    }

    pub fn split(&self) -> (Transaction<K, V>, Transaction<K, V>) {
        let mut gets = Vec::new();
        let mut sets = Vec::new();

        for op in self.ops.iter() {
            match op {
                Op::Set(set) => sets.push(Op::Set(set.clone())),
                Op::Get(get) => gets.push(Op::Get(get.clone())),
            }
        }

        return (Transaction { ops: gets }, Transaction { ops: sets });
    }
}

#[derive(Clone)]
pub struct History<K: Key, V: Value> {
    pub transactions: Vec<Vec<Transaction<K, V>>>,
}

impl<K: Key, V: Value> History<K, V> {
    fn vars(&self) -> HashMap<K, HashSet<usize>> {
        let mut vars = HashMap::new();

        for (index, c) in self.transactions.iter().enumerate() {
            for t in c.iter() {
                for op in t.ops.iter() {
                    match op {
                        Op::Get(get) => {
                            match vars.get_mut(&get.key) {
                                Some(times) => {}
                                None => {
                                    match vars.insert(get.key.clone(), HashSet::new()) {
                                        None => {},
                                        Some(_) => unreachable!(),
                                    }
                                }
                            }
                        }
                        Op::Set(set) => {
                            if let Some(times) = vars.get_mut(&set.key) {
                                times.insert(index);
                            } else {
                                match vars.insert(set.key.clone(), HashSet::new()) {
                                    None => {},
                                    Some(_) => unreachable!(),
                                }
                            }
                        }
                    }
                }
            }
        }

        vars
    }

    pub fn new(mut transactions: Vec<Vec<Transaction<K, V>>>) -> Self {
        Self { transactions }
    }

    fn pre_init(&mut self) {
        let mut vars = self.vars();

        let mut ops = Vec::new();
        for (key, _) in vars.iter() {
            ops.push(Op::Set(Set::new(key.clone(), V::default())))
        }

        let init_transaction = Transaction { ops };
        self.transactions.push(vec![init_transaction]);
    }

    pub fn ser_check(&self) -> bool {
        let mut pre_inited_self = self.clone();
        pre_inited_self.pre_init();
        let mut checker = SerChecker::new(pre_inited_self.transactions.clone());
        checker.check()
    }

    pub fn prefix_check(&self) -> bool {
        let transactions = self.transactions.clone();
        let mut splited_transactions = Vec::new();

        for c in transactions.iter() {
            let mut client = Vec::new();

            for t in c.iter() {
                let (r, w) = t.split();
                client.push(r);
                client.push(w);
            }

            splited_transactions.push(client);
        }

        let mut history = Self::new(splited_transactions);
        history.ser_check()
    }

    pub fn si_check(&self) -> bool {
        let vars_map = self.vars();

        let transactions = self.transactions.clone();
        let mut splited_transactions = Vec::new();

        for (index, c) in transactions.iter().enumerate() {
            let mut client = Vec::new();

            for t in c.iter() {
                let (mut r, mut w) = t.split();

                for op_index in 0..w.ops.len() {
                    let op = &w.ops[op_index];
                    match op {
                        Op::Set(set) => {
                            match vars_map.get(&set.key) {
                                Some(clients) => {
                                    let key = set.key.clone();

                                    r.ops.push(
                                        Op::Set(Set::new(key.generate_guard(index), V::default()))
                                    );
                                    for client in clients.iter() {
                                        if *client != index {
                                            w.ops.push(Op::Set(Set::new(
                                                key.generate_guard(*client), V::abnormal_value()
                                            )))
                                        } else {
                                            w.ops.push(Op::Get(Get::new(
                                                key.generate_guard(*client), V::default()
                                            )))
                                        }
                                    }
                                }
                                None => {
                                    unreachable!();
                                }
                            }
                        }
                        Op::Get(get)=> {
                            unreachable!();
                        }
                    }
                }
                client.push(r);
                client.push(w);
            }

            splited_transactions.push(client);
        }

        let mut history = Self::new(splited_transactions);
        history.ser_check()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! x {
        () => {String::from("x")};
    }

    macro_rules! y {
        () => {String::from("y")};
    }

    #[test]
    fn serializability_check() {
        let t1 = Transaction {
            ops: vec![Op::Set(Set::new(x!(), 1)), Op::Set(Set::new(y!(), 1))],
        };

        let t2 = Transaction {
            ops: vec![
                Op::Get(Get::new(x!(), 1)),
                Op::Get(Get::new(y!(), 1)),
                Op::Set(Set::new(x!(), 2)),
            ],
        };

        let t3 = Transaction {
            ops: vec![
                Op::Get(Get::new(x!(), 1)),
                Op::Get(Get::new(y!(), 1)),
                Op::Set(Set::new(y!(), 2)),
            ],
        };

        let history = History::new(vec![vec![t1], vec![t2], vec![t3]]);

        assert_eq!(history.ser_check(), false);
    }

    #[test]
    fn lost_update() {
        let t1 = Transaction {
            ops: vec![Op::Get(Get::new(x!(), 0)), Op::Set(Set::new(x!(), 1))],
        };

        let t2 = Transaction {
            ops: vec![Op::Get(Get::new(x!(), 0)), Op::Set(Set::new(x!(), 2))],
        };

        let history = History::new(vec![vec![t1], vec![t2]]);

        assert_eq!(history.ser_check(), false);
        assert_eq!(history.si_check(), false);
        assert_eq!(history.prefix_check(), true);
    }

    #[test]
    fn long_fork() {
        let t1 = Transaction {
            ops: vec![Op::Get(Get::new(x!(), 0)), Op::Set(Set::new(x!(), 1))],
        };

        let t2 = Transaction {
            ops: vec![Op::Get(Get::new(y!(), 0)), Op::Set(Set::new(y!(), 1))],
        };

        let t3 = Transaction {
            ops: vec![Op::Get(Get::new(x!(), 1)), Op::Get(Get::new(y!(), 0))],
        };

        let t4 = Transaction {
            ops: vec![Op::Get(Get::new(x!(), 0)), Op::Get(Get::new(y!(), 1))],
        };

        let history = History::new(vec![vec![t1], vec![t2], vec![t3], vec![t4]]);

        assert_eq!(history.ser_check(), false);
        assert_eq!(history.si_check(), false);
        assert_eq!(history.prefix_check(), false);
    }

    #[test]
    fn write_skew() {
        let t1 = Transaction {
            ops: vec![
                Op::Get(Get::new(x!(), 0)),
                Op::Get(Get::new(y!(), 0)),
                Op::Set(Set::new(x!(), 1)),
            ],
        };

        let t2 = Transaction {
            ops: vec![
                Op::Get(Get::new(x!(), 0)),
                Op::Get(Get::new(y!(), 0)),
                Op::Set(Set::new(y!(), 1)),
            ],
        };

        let history = History::new(vec![vec![t1], vec![t2]]);

        assert_eq!(history.ser_check(), false);
        assert_eq!(history.si_check(), true);
        assert_eq!(history.prefix_check(), true);
    }
}
