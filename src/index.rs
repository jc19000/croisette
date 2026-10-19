//! Subscription index: a trie of topic levels with `+` and `#` branches.

use std::collections::HashMap;

/// A subscriber: the core owning its connection, and the connection id on that core.
pub type Sub = (u16, u64);

/// A subscriber with the QoS granted to its subscription.
pub type Entry = (Sub, u8);

#[derive(Default)]
struct Node {
    children: HashMap<Box<str>, Node>,
    plus: Option<Box<Node>>,
    hash: Vec<Entry>,  // subscribers of `<path>/#`
    exact: Vec<Entry>, // subscribers of `<path>`
}

impl Node {
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.plus.is_none() && self.hash.is_empty() && self.exact.is_empty()
    }
}

#[derive(Default)]
pub struct SubIndex {
    root: Node,
    len: usize,
}

impl SubIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of (filter, subscriber) pairs.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds `sub` to `filter` with QoS `qos`; subscribing again to the same filter replaces the
    /// QoS.
    pub fn add(&mut self, filter: &str, sub: Sub, qos: u8) {
        let mut node = &mut self.root;
        let mut levels = filter.split('/').peekable();
        while let Some(level) = levels.next() {
            if level == "#" {
                self.len += put(&mut node.hash, sub, qos);
                return;
            }
            node = if level == "+" {
                node.plus.get_or_insert_with(Default::default)
            } else {
                node.children.entry(level.into()).or_default()
            };
            if levels.peek().is_none() {
                self.len += put(&mut node.exact, sub, qos);
            }
        }
    }

    /// Removes `sub` from `filter`; returns whether it was there.
    pub fn remove(&mut self, filter: &str, sub: Sub) -> bool {
        let levels: Vec<&str> = filter.split('/').collect();
        let removed = remove_at(&mut self.root, &levels, sub);
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// Calls `f` for every subscriber whose filter matches `topic` (once per matching filter),
    /// with the QoS of that subscription.
    pub fn for_each_match(&self, topic: &str, mut f: impl FnMut(Sub, u8)) {
        let levels: Vec<&str> = topic.split('/').collect();
        let dollar = topic.starts_with('$');
        visit(&self.root, &levels, true, dollar, &mut f);
    }
}

fn remove_at(node: &mut Node, levels: &[&str], sub: Sub) -> bool {
    let Some((&level, rest)) = levels.split_first() else { return false };
    if level == "#" {
        return take(&mut node.hash, sub);
    }
    let removed = if level == "+" {
        let Some(child) = node.plus.as_mut() else { return false };
        let removed = if rest.is_empty() { take(&mut child.exact, sub) } else { remove_at(child, rest, sub) };
        if child.is_empty() {
            node.plus = None;
        }
        removed
    } else {
        let Some(child) = node.children.get_mut(level) else { return false };
        let removed = if rest.is_empty() { take(&mut child.exact, sub) } else { remove_at(child, rest, sub) };
        if child.is_empty() {
            node.children.remove(level);
        }
        removed
    };
    removed
}

/// Adds or updates `sub`; returns 1 if it was added.
fn put(entries: &mut Vec<Entry>, sub: Sub, qos: u8) -> usize {
    match entries.iter_mut().find(|e| e.0 == sub) {
        Some(e) => {
            e.1 = qos;
            0
        }
        None => {
            entries.push((sub, qos));
            1
        }
    }
}

fn take(subs: &mut Vec<Entry>, sub: Sub) -> bool {
    match subs.iter().position(|s| s.0 == sub) {
        Some(i) => {
            subs.swap_remove(i);
            true
        }
        None => false,
    }
}

fn visit(node: &Node, levels: &[&str], root: bool, dollar: bool, f: &mut impl FnMut(Sub, u8)) {
    // `#` and `+` at the first level never match topics starting with `$`.
    let wildcards = !(root && dollar);
    if wildcards {
        node.hash.iter().for_each(|&(s, q)| f(s, q));
    }
    let Some((&level, rest)) = levels.split_first() else {
        node.exact.iter().for_each(|&(s, q)| f(s, q));
        return;
    };
    if let Some(child) = node.children.get(level) {
        if rest.is_empty() {
            child.exact.iter().for_each(|&(s, q)| f(s, q));
            child.hash.iter().for_each(|&(s, q)| f(s, q)); // `a/#` matches `a`
        } else {
            visit(child, rest, false, dollar, f);
        }
    }
    if wildcards {
        if let Some(child) = node.plus.as_deref() {
            if rest.is_empty() {
                child.exact.iter().for_each(|&(s, q)| f(s, q));
                child.hash.iter().for_each(|&(s, q)| f(s, q));
            } else {
                visit(child, rest, false, dollar, f);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::matches;

    fn matched(index: &SubIndex, topic: &str) -> Vec<Sub> {
        let mut out = Vec::new();
        index.for_each_match(topic, |s, _| out.push(s));
        out.sort();
        out
    }

    #[test]
    fn agrees_with_the_reference_matcher() {
        let filters = ["a/b", "a/+", "a/#", "#", "+/b", "+/+", "a/b/c", "a/+/c", "+", "$SYS/#", "$SYS/+", "a/b/#", "x"];
        let topics = ["a/b", "a/c", "a", "a/b/c", "b/b", "$SYS/x", "$SYS", "x", "x/y", "a/x/c"];
        let mut index = SubIndex::new();
        for (i, f) in filters.iter().enumerate() {
            index.add(f, (0, i as u64), 0);
        }
        for t in topics {
            let want: Vec<Sub> =
                filters.iter().enumerate().filter(|(_, f)| matches(f, t)).map(|(i, _)| (0, i as u64)).collect();
            assert_eq!(matched(&index, t), want, "topic {t}");
        }
    }

    #[test]
    fn add_remove_and_prune() {
        let mut index = SubIndex::new();
        index.add("a/+/c", (1, 7), 0);
        index.add("a/+/c", (1, 7), 1);
        index.add("a/#", (2, 9), 0);
        assert_eq!(index.len(), 2);
        let mut qos = Vec::new();
        index.for_each_match("a/b/c", |s, q| qos.push((s, q)));
        qos.sort();
        assert_eq!(qos, vec![((1, 7), 1), ((2, 9), 0)], "the second subscription replaced the QoS");
        assert_eq!(matched(&index, "a/b/c"), vec![(1, 7), (2, 9)]);
        assert!(index.remove("a/+/c", (1, 7)));
        assert!(!index.remove("a/+/c", (1, 7)));
        assert!(index.remove("a/#", (2, 9)));
        assert!(index.is_empty());
        assert!(index.root.is_empty(), "empty branches are pruned");
    }
}
