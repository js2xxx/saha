#![allow(unused_unsafe)]
#![feature(build_hasher_simple_hash_one)]
#![feature(once_cell)]
#![feature(type_alias_impl_trait)]
#![cfg_attr(test, feature(test, map_try_insert))]

#[cfg(test)]
extern crate test;

mod adaptive;
mod large;
mod small;
mod small0;

pub use self::adaptive::StringMap;
pub use self::large::StringMap as LargeStringMap;
pub use self::small::StringMap as SmallStringMap;
pub use self::small0::StringMap as Small0StringMap;
