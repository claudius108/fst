use crate::Streamer;
use crate::Set;
use crate::set;

trait IntersectionByPrefixExt<'s> {
    fn intersection_by_prefix<'a>(self, suffix_delimiter: u8) -> IntersectionByPrefix<'a>
    where
        's: 'a;
}

impl<'b> IntersectionByPrefixExt<'b> for crate::set::OpBuilder<'b> {
    #[inline]
    fn intersection_by_prefix<'a>(self, suffix_delimiter: u8) -> IntersectionByPrefix<'a>
    where
        'b: 'a,
    {
        IntersectionByPrefix(self.0.intersection_by_prefix(suffix_delimiter))
    }
}

trait RawIntersectionByPrefixExt<'s> {
    fn intersection_by_prefix<'a>(self, suffix_delimiter: u8) -> RawIntersectionByPrefix<'a>
    where
        's: 'a;
}

impl<'b> RawIntersectionByPrefixExt<'b> for crate::raw::OpBuilder<'b> {
    #[inline]
    fn intersection_by_prefix<'a>(self, suffix_delimiter: u8) -> RawIntersectionByPrefix<'a>
    where
        'b: 'a,
    {
        RawIntersectionByPrefix {
            heap: StreamHeap::new(self.streams, suffix_delimiter),
            outs: vec![],
            cur_slot: None,
            suffix_delimiter,
        }
    }
}

pub struct IntersectionByPrefix<'s>(RawIntersectionByPrefix<'s>);

impl<'a, 's> Streamer<'a> for IntersectionByPrefix<'s> {
    type Item = &'a [u8];

    #[inline]
    fn next(&'a mut self) -> Option<&'a [u8]> {
        self.0.next().map(|(key, _)| key)
    }
}

pub struct RawIntersectionByPrefix<'f> {
    heap: StreamHeap<'f>,
    outs: Vec<crate::raw::IndexedValue>,
    cur_slot: Option<Slot>,
    suffix_delimiter: u8,
}

impl<'a, 'f> Streamer<'a> for RawIntersectionByPrefix<'f> {
    type Item = (&'a [u8], &'a [crate::raw::IndexedValue]);

    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some(slot) = self.cur_slot.take() {
            self.heap.refill(slot);
        }
        loop {
            let slot = match self.heap.pop() {
                None => return None,
                Some(slot) => {
                    let slot_input = slot.input;
                    let slot_input_splitted: Vec<_> =
                        slot_input.split(|i| *i == self.suffix_delimiter).collect();
                    let slot_input_suffix = *slot_input_splitted.get(1).unwrap();

                    let mut slot_by_suffix = Slot::new(slot.idx);
                    slot_by_suffix.set_input(slot_input_suffix);
                    slot_by_suffix.set_output(slot.output);

                    slot_by_suffix
                }
            };
            self.outs.clear();
            self.outs.push(slot.indexed_value());
            let mut popped: usize = 1;
            while let Some(slot2) = self.heap.pop_if_equal_by_suffix(slot.input()) {
                self.outs.push(slot2.indexed_value());
                self.heap.refill(slot2);
                popped += 1;
            }
            if popped < self.heap.num_slots() {
                self.heap.refill(slot);
            } else {
                self.cur_slot = Some(slot);
                let key = self.cur_slot.as_ref().unwrap().input();
                return Some((key, &self.outs));
            }
        }
    }
}

type BoxedStream<'f> = Box<dyn for<'a> Streamer<'a, Item = (&'a [u8], crate::raw::Output)> + 'f>;

struct StreamHeap<'f> {
    rdrs: Vec<BoxedStream<'f>>,
    heap: std::collections::BinaryHeap<Slot>,
    suffix_delimiter: u8,
}

impl<'f> StreamHeap<'f> {
    fn new(streams: Vec<BoxedStream<'f>>, suffix_delimiter: u8) -> StreamHeap<'f> {
        let mut u = StreamHeap {
            rdrs: streams,
            heap: std::collections::BinaryHeap::new(),
            suffix_delimiter,
        };
        for i in 0..u.rdrs.len() {
            u.refill(Slot::new(i));
        }
        u
    }

    fn pop(&mut self) -> Option<Slot> {
        self.heap.pop()
    }

    fn peek_is_duplicate_by_suffix(&self, key: &[u8]) -> bool {
        self.heap
            .peek()
            .map(|s| {
                let slot_input_splitted: Vec<_> = s.input().split(|i| *i == 45).collect();
                let slot_input_suffix = *slot_input_splitted.get(1).unwrap();

                slot_input_suffix == key
            })
            .unwrap_or(false)
    }

    fn pop_if_equal_by_suffix(&mut self, key: &[u8]) -> Option<Slot> {
        if self.peek_is_duplicate_by_suffix(key) {
            self.pop()
        } else {
            None
        }
    }

    fn num_slots(&self) -> usize {
        self.rdrs.len()
    }

    fn refill(&mut self, mut slot: Slot) {
        if let Some((input, output)) = self.rdrs[slot.idx].next() {
            slot.set_input(input);
            slot.set_output(output);
            self.heap.push(slot);
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Slot {
    idx: usize,
    input: Vec<u8>,
    output: crate::raw::Output,
}

impl Slot {
    fn new(rdr_idx: usize) -> Slot {
        Slot {
            idx: rdr_idx,
            input: Vec::with_capacity(64),
            output: crate::raw::Output::zero(),
        }
    }

    fn indexed_value(&self) -> crate::raw::IndexedValue {
        crate::raw::IndexedValue {
            index: self.idx,
            value: self.output.value(),
        }
    }

    fn input(&self) -> &[u8] {
        &self.input
    }

    fn set_input(&mut self, input: &[u8]) {
        self.input.clear();
        self.input.extend(input);
    }

    fn set_output(&mut self, output: crate::raw::Output) {
        self.output = output;
    }
}

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Slot) -> Option<std::cmp::Ordering> {
        (&self.input, self.output)
            .partial_cmp(&(&other.input, other.output))
            .map(|ord| ord.reverse())
    }
}

impl Ord for Slot {
    fn cmp(&self, other: &Slot) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[test]
fn fst_intersection_1_test() {
    use crate::{set, Set};
    use std::time::Instant;

    let set_1 = Set::from_iter(vec!["1-1", "1-2", "1-3"]).unwrap();
    let set_2 = Set::from_iter(vec!["2-2", "2-4"]).unwrap();

    let now = Instant::now();
    let mut stream = set::OpBuilder::new()
        .add(set_1.into_stream())
        .add(set_2.into_stream())
        .intersection_by_prefix(45);
    let elapsed = now.elapsed();
    println!("Time for intersection: {:.2?}", elapsed);

    let mut keys = vec![];
    while let Some(key) = stream.next() {
        keys.push(String::from_utf8(key.to_vec()).unwrap());
    }
    println!("{:?}", keys);
}

#[test]
fn fst_intersection_2_test() {
    use crate::automaton::Str;
    use crate::{set, Automaton, Set};
    use memmap::Mmap;
    use std::fs::File;
    use std::time::Instant;

    let index_dir_path = "/home/claudius/workspace/repositories/git/gitlab.com/claudius.teodorescu/index-search-engine/tests/data/".to_string();

    let locators_data = unsafe {
        Mmap::map(&File::open(index_dir_path.clone() + "new-locators.fst").unwrap()).unwrap()
    };
    let set = Set::new(locators_data).unwrap();

    // get the first stream
    let query_1 = Str::new("1-").starts_with();
    let stream_1 = set.search(query_1).into_stream();

    let query_2 = Str::new("2-").starts_with();
    let stream_2 = set.search(query_2).into_stream();

    let now = Instant::now();
    let mut stream = set::OpBuilder::new()
        .add(stream_1)
        .add(stream_2)
        .intersection_by_prefix(45);
    let elapsed = now.elapsed();
    println!("Time for intersection: {:.2?}", elapsed);

    let mut keys = vec![];
    while let Some(key) = stream.next() {
        keys.push(String::from_utf8(key.to_vec()).unwrap());
    }
    println!("{:?}", keys.len());
}
