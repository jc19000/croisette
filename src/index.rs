//! Subscription index: a trie of topic levels with `+` and `#` branches.

use std::collections::HashMap;

/// A subscriber: the core owning its connection, and the connection id on that core.
pub type Sub = (u16, u64);

#[derive(Default)]
struct Node {
    children: HashMap<Box<str>, Node>,
    plus: Option<Box<Node>>,
    hash: Vec<Sub>,  // subscribers of `<path>/#`
    exact: Vec<Sub>, // subscribers of `<path>`
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

    /// Adds `sub` to `filter`; adding the same pair twice keeps one.
    pub fn add(&mut self, filter: &str, sub: Sub) {
        let mut node = &mut self.root;
        let mut levels = filter.split('/').peekable();
        while let Some(level) = levels.next() {
            if level == "#" {
                if !node.hash.contains(&sub) {
                    node.hash.push(sub);
                    self.len += 1;
                }
                return;
            }
            node = if level == "+" {
                node.plus.get_or_insert_with(Default::default)
            } else {
                node.children.entry(level.into()).or_default()
            };
            if levels.peek().is_none() && !node.exact.contains(&sub) {
                node.exact.push(sub);
                self.len += 1;
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

    /// Calls `f` for every subscriber whose filter matches `topic` (once per matching filter).
    pub fn for_each_match(&self, topic: &str, mut f: impl FnMut(Sub)) {
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

fn take(subs: &mut Vec<Sub>, sub: Sub) -> bool {
    match subs.iter().position(|s| *s == sub) {
        Some(i) => {
            subs.swap_remove(i);
            true
        }
        None => false,
    }
}

fn visit(node: &Node, levels: &[&str], root: bool, dollar: bool, f: &mut impl FnMut(Sub)) {
    // `#` and `+` at the first level never match topics starting with `$`.
    let wildcards = !(root && dollar);
    if wildcards {
        node.hash.iter().for_each(|s| f(*s));
    }
    let Some((&level, rest)) = levels.split_first() else {
        node.exact.iter().for_each(|s| f(*s));
        return;
    };
    if let Some(child) = node.children.get(level) {
        if rest.is_empty() {
            child.exact.iter().for_each(|s| f(*s));
            child.hash.iter().for_each(|s| f(*s)); // `a/#` matches `a`
        } else {
            visit(child, rest, false, dollar, f);
        }
    }
    if wildcards {
        if let Some(child) = node.plus.as_deref() {
            if rest.is_empty() {
                child.exact.iter().for_each(|s| f(*s));
                child.hash.iter().for_each(|s| f(*s));
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
        index.for_each_match(topic, |s| out.push(s));
        out.sort();
        out
    }

    #[test]
    fn agrees_with_the_reference_matcher() {
        let filters = ["a/b", "a/+", "a/#", "#", "+/b", "+/+", "a/b/c", "a/+/c", "+", "$SYS/#", "$SYS/+", "a/b/#", "x"];
        let topics = ["a/b", "a/c", "a", "a/b/c", "b/b", "$SYS/x", "$SYS", "x", "x/y", "a/x/c"];
        let mut index = SubIndex::new();
        for (i, f) in filters.iter().enumerate() {
            index.add(f, (0, i as u64));
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
        index.add("a/+/c", (1, 7));
        index.add("a/+/c", (1, 7));
        index.add("a/#", (2, 9));
        assert_eq!(index.len(), 2);
        assert_eq!(matched(&index, "a/b/c"), vec![(1, 7), (2, 9)]);
        assert!(index.remove("a/+/c", (1, 7)));
        assert!(!index.remove("a/+/c", (1, 7)));
        assert!(index.remove("a/#", (2, 9)));
        assert!(index.is_empty());
        assert!(index.root.is_empty(), "empty branches are pruned");
    }
}
