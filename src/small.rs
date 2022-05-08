use bumpalo::Bump;

use crate::common;

pub struct SlotData<T, const N: usize> {
    key: [u8; N],
    len: usize,
    value: T,
}

impl<'a, T, const N: usize> common::SlotData<'a> for SlotData<T, N> {
    type Value = T;

    #[inline]
    fn new(_: &'a Bump, key: &[u8], _hash: u64, value: Self::Value) -> Self {
        let mut data = SlotData {
            key: [0; N],
            len: key.len(),
            value,
        };
        data.key[..data.len].copy_from_slice(key);
        data
    }

    #[inline]
    fn key(&self) -> &[u8] {
        &self.key[..self.len]
    }

    #[inline]
    fn hash(&self) -> Option<u64> {
        None
    }

    #[inline]
    fn value(&self) -> &Self::Value {
        &self.value
    }

    #[inline]
    fn kv_mut(&mut self) -> (&[u8], &mut Self::Value) {
        (&self.key[..self.len], &mut self.value)
    }

    #[inline]
    fn into_value(self) -> Self::Value {
        self.value
    }

    #[inline]
    fn into_kv(self, key_alloc: &'a Bump) -> (&'a [u8], Self::Value) {
        (
            key_alloc.alloc_slice_copy(&self.key[..self.len]),
            self.value,
        )
    }
}

pub type StringMap<'a, T, const N: usize> = common::StringMap<'a, SlotData<T, N>>;

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn test_hash_map() {
        let bump = Bump::new();
        let mut map = StringMap::<u64, 16>::new(&bump);
        let mut cmp = HashMap::new();
        let hasher = RandomState::default();

        for _ in 0..10 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            let a1 = map.insert(&key, hash, value, &hasher);
            let a2 = cmp.insert(key.to_vec(), value);
            assert_eq!(a1, a2);
        }

        // Test conflict conditions
        for _ in 0..1000 {
            let value = rand::random::<u16>() as u64;
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            let a1 = map.insert(&key, hash, value, &hasher);
            let a2 = cmp.insert(key.to_vec(), value);
            assert_eq!(a1, a2);
        }

        for (k, v) in map {
            let value = cmp.remove(k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }

    #[test]
    fn test_conflict() {
        let bump = Bump::new();
        let mut map = StringMap::<u64, 16>::new(&bump);
        let hasher = RandomState::default();

        let value = 111111111111111u64;
        let key = value.to_ne_bytes();
        let hash = hasher.hash_one(&key as &[u8]);

        let a = map.insert(&key, hash, value, &hasher);
        assert_eq!(a, None);

        for _ in 0..7 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(&key as &[u8]);

            map.insert(&key, hash, value, &hasher);
        }
        let a = map.insert(&key, hash, value, &hasher);
        assert_eq!(a, Some(value));
    }
}
