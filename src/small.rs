use crate::common;

pub struct SlotData<T, const N: usize> {
    key: [u8; N],
    len: usize,
    value: T,
}

impl<T, const N: usize> common::SlotData for SlotData<T, N> {
    type Value = T;

    fn new(key: &[u8], _hash: u64, value: Self::Value) -> Self {
        let mut data = SlotData {
            key: [0; N],
            len: key.len(),
            value,
        };
        data.key[..data.len].copy_from_slice(key);
        data
    }

    fn key(&self) -> &[u8] {
        &self.key[..self.len]
    }

    fn hash(&self) -> Option<u64> {
        None
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }

    fn kv_mut(&mut self) -> (&[u8], &mut Self::Value) {
        (&self.key[..self.len], &mut self.value)
    }

    fn into_value(self) -> Self::Value {
        self.value
    }

    fn into_kv(self) -> (Vec<u8>, Self::Value) {
        (Vec::from(&self.key[..self.len]), self.value)
    }
}

pub type StringMap<T, const N: usize> = common::StringMap<SlotData<T, N>>;

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn test_hash_map() {
        let mut map = StringMap::<u64, 16>::new();
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

        for (k, &v) in map.iter() {
            let value = cmp.remove(k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }

    #[test]
    fn test_conflict() {
        let mut map = StringMap::<u64, 16>::new();
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
