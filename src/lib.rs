#![allow(unused_unsafe)]
#![feature(associated_type_bounds)]
#![feature(build_hasher_simple_hash_one)]
#![feature(once_cell)]
#![feature(type_alias_impl_trait)]
#![cfg_attr(test, feature(test, map_try_insert))]

#[cfg(test)]
extern crate test;

mod adaptive;
mod array;
mod common;
mod large;
mod small;

pub use self::adaptive::StringMap;
pub use self::array::StringMap as ArrayStringMap;
pub use self::common::Slot;
pub use self::large::StringMap as LargeStringMap;
pub use self::small::StringMap as SmallStringMap;
