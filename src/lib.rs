#![feature(trait_upcasting)]
#![allow(incomplete_features)]
#![feature(iterator_try_collect)]
#![feature(iter_next_chunk)]

pub mod error;
pub mod proto;
pub mod reader;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
