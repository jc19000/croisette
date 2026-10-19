//! Subscription index: a trie of topic levels with `+` and `#` branches.
//!
//! Shared subscriptions (`$share/<group>/<filter>`) sit where `<filter>` ends, as groups: a
//! message matching the filter goes to one member of each group, in turn.

use std::cell::Cell;
use std::collections::HashMap;

/// A subscriber: the core owning its connection, and the connection id on that core.
pub type Sub = (u16, u64);

/// A subscriber with the QoS granted to its subscription.
pub type Entry = (Sub, u8);

/// Splits `$share/<group>/<filter>` into the group and the filter.
pub fn shared(filter: &str) -> Option<(&str, &str)> {
    filter.strip_prefix("$share/")?.split_once('/')
}

#[derive(Default)]
struct Subs {
    plain: Vec<Entry>,
    groups: Vec<Group>,
}

struct Group {
    name: Box<str>,
    members: Vec<Entry>,
    next: Cell<usize>,
}

impl Subs {
    fn is_empty(&self) -> bool {
        self.plain.is_empty() && self.groups.is_empty()
    }

    /// Adds or updates `sub`; returns 1 if it was added.
    fn put(&mut self, group: Option<&str>, sub: Sub, qos: u8) -> usize {
        let entries = match group {
            None => &mut self.plain,
            Some(name) => match self.groups.iter().position(|g| &*g.name == name) {
                Some(i) => &mut self.groups[i].members,
                None => {
                    self.groups.push(Group { name: name.into(), members: Vec::new(), next: Cell::new(0) });
                    &mut self.groups.last_mut().unwrap().members
                }
            },
        };
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

    fn take(&mut self, group: Option<&str>, sub: Sub) -> bool {
        let (entries, g) = match group {
            None => (&mut self.plain, None),
            Some(name) => match self.groups.iter().position(|g| &*g.name == name) {
                Some(i) => (&mut self.groups[i].members, Some(i)),
                None => return false,
            },
        };
        let Some(at) = entries.iter().position(|s| s.0 == sub) else { return false };
        entries.swap_remove(at);
        if let Some(i) = g {
            if self.groups[i].members.is_empty() {
                self.groups.swap_remove(i);
            }
        }
        true
    }

    /// Every plain subscriber, and one member of each group.
    #[inline]
    fn emit(&self, f: &mut impl FnMut(Sub, u8)) {
        self.plain.iter().for_each(|&(s, q)| f(s, q));
        for g in &self.groups {
            let i = g.next.get() % g.members.len();
            g.next.set(i + 1);
            let (s, q) = g.members[i];
            f(s, q);
        }
    }
}

#[derive(Default)]
struct Node {
    children: HashMap<Box<str>, Node>,
    plus: Option<Box<Node>>,
    hash: Subs,  // subscribers of `<path>/#`
    exact: Subs, // subscribers of `<path>`
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
    /// QoS. A `$share/<group>/<filter>` filter makes `sub` a member of the group.
    pub fn add(&mut self, filter: &str, sub: Sub, qos: u8) {
        let (group, filter) = match shared(filter) {
            Some((g, f)) => (Some(g), f),
            None => (None, filter),
        };
        let mut node = &mut self.root;
        let mut levels = filter.split('/').peekable();
        while let Some(level) = levels.next() {
            if level == "#" {
                self.len += node.hash.put(group, sub, qos);
                return;
            }
            node = if level == "+" {
                node.plus.get_or_insert_with(Default::default)
            } else {
                node.children.entry(level.into()).or_default()
            };
            if levels.peek().is_none() {
                self.len += node.exact.put(group, sub, qos);
            }
        }
    }

    /// Removes `sub` from `filter`; returns whether it was there.
    pub fn remove(&mut self, filter: &str, sub: Sub) -> bool {
        let (group, filter) = match shared(filter) {
            Some((g, f)) => (Some(g), f),
            None => (None, filter),
        };
        let levels: Vec<&str> = filter.split('/').collect();
        let removed = remove_at(&mut self.root, &levels, group, sub);
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// Calls `f` for every subscriber whose filter matches `topic` (once per matching filter, and
    /// for one member of each matching group), with the QoS of that subscription.
    pub fn for_each_match(&self, topic: &str, mut f: impl FnMut(Sub, u8)) {
        let levels: Vec<&str> = topic.split('/').collect();
        let dollar = topic.starts_with('$');
        visit(&self.root, &levels, true, dollar, &mut f);
    }
}

fn remove_at(node: &mut Node, levels: &[&str], group: Option<&str>, sub: Sub) -> bool {
    let Some((&level, rest)) = levels.split_first() else { return false };
    if level == "#" {
        return node.hash.take(group, sub);
    }
    let removed = if level == "+" {
        let Some(child) = node.plus.as_mut() else { return false };
        let removed = if rest.is_empty() { child.exact.take(group, sub) } else { remove_at(child, rest, group, sub) };
        if child.is_empty() {
            node.plus = None;
        }
        removed
    } else {
        let Some(child) = node.children.get_mut(level) else { return false };
        let removed = if rest.is_empty() { child.exact.take(group, sub) } else { remove_at(child, rest, group, sub) };
        if child.is_empty() {
            node.children.remove(level);
        }
        removed
    };
    removed
}

fn visit(node: &Node, levels: &[&str], root: bool, dollar: bool, f: &mut impl FnMut(Sub, u8)) {
    // `#` and `+` at the first level never match topics starting with `$`.
    let wildcards = !(root && dollar);
    if wildcards {
        node.hash.emit(f);
    }
    let Some((&level, rest)) = levels.split_first() else {
        node.exact.emit(f);
        return;
    };
    if let Some(child) = node.children.get(level) {
        if rest.is_empty() {
            child.exact.emit(f);
            child.hash.emit(f); // `a/#` matches `a`
        } else {
            visit(child, rest, false, dollar, f);
        }
    }
    if wildcards {
        if let Some(child) = node.plus.as_deref() {
            if rest.is_empty() {
                child.exact.emit(f);
                child.hash.emit(f);
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

    #[test]
    fn shared_groups_take_turns() {
        let mut index = SubIndex::new();
        index.add("$share/g/a/+", (0, 1), 0);
        index.add("$share/g/a/+", (0, 2), 0);
        index.add("$share/h/a/#", (0, 3), 0);
        index.add("a/b", (0, 4), 0);
        let mut got = Vec::new();
        for _ in 0..4 {
            got.push(matched(&index, "a/b"));
        }
        assert_eq!(
            got,
            vec![
                vec![(0, 1), (0, 3), (0, 4)],
                vec![(0, 2), (0, 3), (0, 4)],
                vec![(0, 1), (0, 3), (0, 4)],
                vec![(0, 2), (0, 3), (0, 4)]
            ]
        );
        assert!(index.remove("$share/g/a/+", (0, 1)));
        assert_eq!(matched(&index, "a/b"), vec![(0, 2), (0, 3), (0, 4)]);
        assert!(index.remove("$share/g/a/+", (0, 2)));
        assert!(index.remove("$share/h/a/#", (0, 3)));
        assert!(index.remove("a/b", (0, 4)));
        assert!(index.is_empty() && index.root.is_empty());
    }
}
