use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

use bumpalo::Bump;

use crate::{LargeStringMap, SmallStringMap};

#[derive(Debug, Clone, Copy)]
pub enum KeyRef<'a> {
    None,
    // S0([u8; 2]),
    S8(&'a [u8]),
    S16(&'a [u8]),
    S24(&'a [u8]),
    Large(&'a [u8]),
}

impl<'a> KeyRef<'a> {
    pub fn key(&self) -> &[u8] {
        match self {
            KeyRef::None => &[],
            KeyRef::S8(key) | KeyRef::S16(key) | KeyRef::S24(key) | KeyRef::Large(key) => key,
        }
    }
}

impl<'a> From<&'a [u8]> for KeyRef<'a> {
    fn from(key: &[u8]) -> KeyRef {
        let len = key.len();
        match key {
            [] => KeyRef::None,
            key if len <= 8 => KeyRef::S8(key),
            key if len <= 16 => KeyRef::S16(key),
            key if len <= 24 => KeyRef::S24(key),
            key => KeyRef::Large(key),
        }
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for KeyRef<'a> {
    fn from(key: &'a [u8; N]) -> Self {
        match key as &[u8] {
            [] => KeyRef::None,
            key if N <= 8 => KeyRef::S8(key),
            key if N <= 16 => KeyRef::S16(key),
            key if N <= 24 => KeyRef::S24(key),
            key => KeyRef::Large(key),
        }
    }
}

impl<'a> Hash for KeyRef<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            KeyRef::None => (&[] as &[u8]).hash(state),
            KeyRef::S8(key) | KeyRef::S16(key) | KeyRef::S24(key) | KeyRef::Large(key) => {
                (key as &[u8]).hash(state)
            }
        }
    }
}

pub struct StringMap<'a, T, S> {
    none_key: Option<T>,
    small8: SmallStringMap<'a, T, 8>,
    small16: SmallStringMap<'a, T, 16>,
    small24: SmallStringMap<'a, T, 24>,
    large: LargeStringMap<'a, T>,
    hasher: S,
}

impl<'a, T, S> StringMap<'a, T, S> {
    pub fn with_hasher(hasher: S, key_alloc: &'a Bump) -> Self {
        StringMap {
            none_key: None,
            small8: SmallStringMap::new(key_alloc),
            small16: SmallStringMap::new(key_alloc),
            small24: SmallStringMap::new(key_alloc),
            large: LargeStringMap::new(key_alloc),
            hasher,
        }
    }

    pub fn hasher(&self) -> &S {
        &self.hasher
    }

    pub fn len(&self) -> usize {
        (self.none_key.is_some() as usize)
            + self.small8.len()
            + self.small16.len()
            + self.small24.len()
            + self.large.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.none_key.is_none()
            && self.small8.is_empty()
            && self.small16.is_empty()
            && self.small24.is_empty()
            && self.large.is_empty()
    }
}

impl<'a, T> StringMap<'a, T, RandomState> {
    pub fn new(key_alloc: &'a Bump) -> Self {
        Self::with_hasher(RandomState::new(), key_alloc)
    }
}

impl<'a, T: Hash, S: BuildHasher> StringMap<'a, T, S> {
    pub fn get_hashed(&self, key: KeyRef, hash: u64) -> Option<&T> {
        match key {
            KeyRef::None => self.none_key.as_ref(),
            KeyRef::S8(key) => self.small8.get(key, hash),
            KeyRef::S16(key) => self.small16.get(key, hash),
            KeyRef::S24(key) => self.small24.get(key, hash),
            KeyRef::Large(key) => self.large.get(key, hash),
        }
    }

    pub fn get(&self, key: KeyRef) -> Option<&T> {
        self.get_hashed(key, self.hasher.hash_one(key))
    }

    pub fn get_mut_hashed(&mut self, key: KeyRef, hash: u64) -> Option<&mut T> {
        match key {
            KeyRef::None => self.none_key.as_mut(),
            KeyRef::S8(key) => self.small8.get_mut(key, hash),
            KeyRef::S16(key) => self.small16.get_mut(key, hash),
            KeyRef::S24(key) => self.small24.get_mut(key, hash),
            KeyRef::Large(key) => self.large.get_mut(key, hash),
        }
    }

    pub fn get_mut(&mut self, key: KeyRef) -> Option<&mut T> {
        self.get_mut_hashed(key, self.hasher.hash_one(key))
    }

    pub fn insert_hashed(&mut self, key: KeyRef, hash: u64, value: T) -> Option<T> {
        match key {
            KeyRef::None => self.none_key.replace(value),
            KeyRef::S8(key) => self.small8.insert(key, hash, value, &self.hasher),
            KeyRef::S16(key) => self.small16.insert(key, hash, value, &self.hasher),
            KeyRef::S24(key) => self.small24.insert(key, hash, value, &self.hasher),
            KeyRef::Large(key) => self.large.insert(key, hash, value, &self.hasher),
        }
    }

    pub fn insert(&mut self, key: KeyRef, value: T) -> Option<T> {
        self.insert_hashed(key, self.hasher.hash_one(key), value)
    }

    pub fn try_insert_hashed(&mut self, key: KeyRef, hash: u64, value: T) -> Option<(&mut T, T)> {
        match key {
            KeyRef::None => match &mut self.none_key {
                Some(s) => Some((s, value)),
                slot @ None => {
                    *slot = Some(value);
                    None
                }
            },
            KeyRef::S8(key) => self.small8.try_insert(key, hash, value, &self.hasher),
            KeyRef::S16(key) => self.small16.try_insert(key, hash, value, &self.hasher),
            KeyRef::S24(key) => self.small24.try_insert(key, hash, value, &self.hasher),
            KeyRef::Large(key) => self.large.try_insert(key, hash, value, &self.hasher),
        }
    }

    pub fn try_insert(&mut self, key: KeyRef, value: T) -> Option<(&mut T, T)> {
        self.try_insert_hashed(key, self.hasher.hash_one(key), value)
    }

    pub fn remove_hashed(&mut self, key: KeyRef, hash: u64) -> Option<T> {
        match key {
            KeyRef::None => self.none_key.take(),
            KeyRef::S8(key) => self.small8.remove(key, hash, &self.hasher),
            KeyRef::S16(key) => self.small16.remove(key, hash, &self.hasher),
            KeyRef::S24(key) => self.small24.remove(key, hash, &self.hasher),
            KeyRef::Large(key) => self.large.remove(key, hash, &self.hasher),
        }
    }

    pub fn remove(&mut self, key: KeyRef) -> Option<T> {
        self.remove_hashed(key, self.hasher.hash_one(key))
    }
}

impl<'a, T, S> StringMap<'a, T, S> {
    pub fn iter(&self) -> impl Iterator<Item = (KeyRef, &T)> {
        { self.none_key.iter().map(|value| (KeyRef::None, value)) }
            .chain(
                self.small8
                    .iter()
                    .map(|(key, value)| (KeyRef::S8(key), value)),
            )
            .chain(
                self.small16
                    .iter()
                    .map(|(key, value)| (KeyRef::S16(key), value)),
            )
            .chain(
                self.small24
                    .iter()
                    .map(|(key, value)| (KeyRef::S24(key), value)),
            )
            .chain(
                self.large
                    .iter()
                    .map(|(key, value)| (KeyRef::Large(key), value)),
            )
    }

    pub fn iter_mut(&'a mut self) -> impl Iterator<Item = (KeyRef, &mut T)> + 'a {
        { self.none_key.iter_mut().map(|value| (KeyRef::None, value)) }
            .chain(
                self.small8
                    .iter_mut()
                    .map(|(key, value)| (KeyRef::S8(key), value)),
            )
            .chain(
                self.small16
                    .iter_mut()
                    .map(|(key, value)| (KeyRef::S16(key), value)),
            )
            .chain(
                self.small24
                    .iter_mut()
                    .map(|(key, value)| (KeyRef::S24(key), value)),
            )
            .chain(
                self.large
                    .iter_mut()
                    .map(|(key, value)| (KeyRef::Large(key), value)),
            )
    }
}

impl<'a, T: 'a, S> IntoIterator for StringMap<'a, T, S> {
    type Item = (&'a [u8], T);

    type IntoIter = impl Iterator<Item = (&'a [u8], T)> + 'a;

    fn into_iter(self) -> Self::IntoIter {
        { self.none_key.into_iter().map(|value| (&[] as _, value)) }
            .chain(self.small8.into_iter())
            .chain(self.small16.into_iter())
            .chain(self.small24.into_iter())
            .chain(self.large.into_iter())
    }
}

impl<'a, T, S> IntoIterator for &'a StringMap<'a, T, S> {
    type Item = (KeyRef<'a>, &'a T);

    type IntoIter = impl Iterator<Item = (KeyRef<'a>, &'a T)>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T, S> IntoIterator for &'a mut StringMap<'a, T, S> {
    type Item = (KeyRef<'a>, &'a mut T);

    type IntoIter = impl Iterator<Item = (KeyRef<'a>, &'a mut T)>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_hash_map() {
        let bump = Bump::new();
        let mut map = StringMap::new(&bump);
        let mut cmp = HashMap::new();

        for _ in 0..50000 {
            let value = rand::random::<[u8; 10]>();

            map.insert(KeyRef::from(&value), Vec::from(value));
            cmp.insert(value.into_iter().collect::<Vec<_>>(), Vec::from(value));
        }

        // Test conflict conditions
        for _ in 0..100000 {
            let value = rand::random::<[u8; 4]>();

            map.insert(KeyRef::from(&value), Vec::from(value));
            cmp.insert(value.into_iter().collect::<Vec<_>>(), Vec::from(value));
        }

        for (k, v) in map.iter() {
            let value = cmp.remove(k.key());
            assert_eq!(value.as_ref(), Some(v));
        }
        assert!(cmp.is_empty());
    }

    fn gen_record<'a, S: BuildHasher>(hasher: &S, bump: &'a Bump) -> (&'a [u8], u64) {
        let len = rand::random::<usize>() % 1000;
        let key = bump.alloc_slice_fill_with(len, |_| rand::random());

        let hash = hasher.hash_one(KeyRef::from(&*key));

        (key, hash)
    }

    fn gen_group_data<'a, S: BuildHasher>(hasher: &S, bump: &'a Bump) -> Vec<(&'a [u8], u64)> {
        let mut data = vec![];
        for _ in 0..10000 {
            data.push(gen_record(hasher, bump))
        }
        data
    }

    #[bench]
    fn bench_group(bencher: &mut test::Bencher) {
        bencher.iter(|| {
            let bump = Bump::new();
            let mut map = StringMap::new(&bump);

            let data = gen_group_data(map.hasher(), &bump);

            data.into_iter().for_each(|(key, hash)| {
                if let Some((count, _)) = map.try_insert_hashed(KeyRef::from(key), hash, 1) {
                    *count += 1;
                }
            });

            for (key, value) in map {
                let _ = key;
                let _ = value;
            }
        })
    }

    #[bench]
    fn bench_group_cmp(bencher: &mut test::Bencher) {
        bencher.iter(|| {
            let bump = Bump::new();
            let mut map = HashMap::new();

            let data = gen_group_data(map.hasher(), &bump);

            data.into_iter().for_each(|(key, _)| {
                if let Err(mut err) = map.try_insert(key, 1) {
                    *err.entry.get_mut() += 1;
                }
            });

            for (key, value) in map {
                let _ = key;
                let _ = value;
            }
        })
    }

    #[allow(clippy::type_complexity)]
    fn gen_join_data<'a, S: BuildHasher>(hasher: &S, bump: &'a Bump) -> [Vec<(&'a [u8], u64)>; 2] {
        let data1 = gen_group_data(hasher, bump);

        let mut data2 = vec![];

        for _ in 0..10000 {
            data2.push(if rand::random() {
                gen_record(hasher, bump)
            } else {
                *data1.get(rand::random::<usize>() % data1.len()).unwrap()
            })
        }

        [data1, data2]
    }

    #[bench]
    fn bench_join(bencher: &mut test::Bencher) {
        bencher.iter(|| {
            let bump = Bump::new();
            let mut map = StringMap::new(&bump);
            let [data1, data2] = gen_join_data(map.hasher(), &bump);

            data1.into_iter().for_each(|(key, hash)| {
                map.try_insert_hashed(KeyRef::from(key), hash, ());
            });

            let mut data = vec![];
            data2.into_iter().for_each(|(key, hash)| {
                if map.get_hashed(KeyRef::from(key), hash).is_some() {
                    data.push(key);
                }
            });

            for item in data {
                let _ = item;
            }
        })
    }

    #[bench]
    fn bench_join_cmp(bencher: &mut test::Bencher) {
        bencher.iter(|| {
            let bump = Bump::new();
            let mut map = HashMap::new();
            let [data1, data2] = gen_join_data(map.hasher(), &bump);

            data1.into_iter().for_each(|(key, _)| {
                let _ = map.try_insert(key, ());
            });

            let mut data = vec![];
            data2.into_iter().for_each(|(key, _)| {
                if map.contains_key(&key) {
                    data.push(key);
                }
            });

            for item in data {
                let _ = item;
            }
        })
    }
}
