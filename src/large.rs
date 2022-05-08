use bumpalo::Bump;

use crate::common;

pub struct SlotData<'a, T> {
    hash: u64,
    key: &'a [u8],
    value: T,
}

impl<'a, T> common::SlotData<'a> for SlotData<'a, T> {
    type Value = T;

    #[inline]
    fn new(key_alloc: &'a Bump, key: &[u8], hash: u64, value: Self::Value) -> Self {
        SlotData {
            hash,
            key: key_alloc.alloc_slice_copy(key),
            value,
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.key
    }

    #[inline]
    fn hash(&self) -> Option<u64> {
        Some(self.hash)
    }

    #[inline]
    fn value(&self) -> &Self::Value {
        &self.value
    }

    #[inline]
    fn kv_mut(&mut self) -> (&[u8], &mut Self::Value) {
        (self.key, &mut self.value)
    }

    #[inline]
    fn into_value(self) -> Self::Value {
        self.value
    }

    #[inline]
    fn into_kv(self, _: &'a Bump) -> (&'a [u8], Self::Value) {
        (self.key, self.value)
    }
}

pub type StringMap<'a, T> = common::StringMap<'a, SlotData<'a, T>>;

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn test_hash_map() {
        let bump = Bump::new();
        let mut map = StringMap::new(&bump);
        let mut cmp = HashMap::new();
        let hasher = RandomState::default();

        for _ in 0..50000 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            map.insert(&key, hash, value, &hasher);
            cmp.insert(key.to_vec(), value);
        }

        // Test conflict conditions
        for _ in 0..100000 {
            let value = rand::random::<u16>() as u64;
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            map.insert(&key, hash, value, &hasher);
            cmp.insert(key.to_vec(), value);
        }

        for (k, v) in map {
            let value = cmp.remove(k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }
}
