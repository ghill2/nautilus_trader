// use std::cmp::Ordering;
// use std::collections::BinaryHeap;
// use std::iter::Peekable;

// // Define a custom comparator for the elements in the binary heap
// struct KMergeComparator<T>
// where
//     T: Ord,
// {
//     value: T,
// }

// impl<T> PartialEq for KMergeComparator<T>
// where
//     T: Ord,
// {
//     fn eq(&self, other: &Self) -> bool {
//         self.value.eq(&other.value)
//     }
// }

// impl<T> Eq for KMergeComparator<T> where T: Ord {}

// impl<T> PartialOrd for KMergeComparator<T>
// where
//     T: Ord,
// {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         self.value.partial_cmp(&other.value)
//     }
// }

// impl<T> Ord for KMergeComparator<T>
// where
//     T: Ord,
// {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.value.cmp(&other.value)
//     }
// }

// // KMerge struct holds the input streams and the binary heap
// struct KMerge<I, T>
// where
//     I: Iterator<Item = T>,
//     T: Ord,
// {
//     streams: Vec<Peekable<I>>,
//     heap: BinaryHeap<KMergeComparator<T>>,
// }

// impl<I, T> KMerge<I, T>
// where
//     I: Iterator<Item = T>,
//     T: Ord,
// {
//     // Create a new KMerge instance
//     fn new(streams: Vec<I>) -> Self {
//         let mut heap = BinaryHeap::new();
//         let mut peekable_streams: Vec<Peekable<I>> = Vec::new();

//         // Push the first element from each stream into the binary heap
//         for stream in streams {
//             let mut peekable_stream = stream.peekable();
//             if let Some(element) = peekable_stream.peek().cloned() {
//                 heap.push(KMergeComparator { value: element });
//             }
//             peekable_streams.push(peekable_stream);
//         }

//         KMerge {
//             streams: peekable_streams,
//             heap,
//         }
//     }
// }

// impl<I, T> Iterator for KMerge<I, T>
// where
//     I: Iterator<Item = T>,
//     T: Ord,
// {
//     type Item = T;

//     fn next(&mut self) -> Option<Self::Item> {
//         if let Some(KMergeComparator { value }) = self.heap.pop() {
//             // Find the index of the stream that contains the next element
//             let mut index = 0;
//             while index < self.streams.len() {
//                 if self.streams[index].peek() == Some(&value) {
//                     break;
//                 }
//                 index += 1;
//             }

//             // Pop the next element from the corresponding stream and push the next element into the heap
//             if let Some(element) = self.streams[index].next() {
//                 if let Some(next_element) = self.streams[index].peek() {
//                     self.heap.push(KMergeComparator {
//                         value: next_element.clone(),
//                     });
//                 }
//                 return Some(element);
//             }
//         }
//         None
//     }
// }

