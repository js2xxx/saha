use std::fmt;
use std::hash::Hash;
use std::iter::FusedIterator;
use std::{hint, mem};

const LOAD_FACTOR_N: usize = 3;
const LOAD_FACTOR_D: usize = 2;
const MIN_CAPACITY: usize = 4;

struct SlotData<T> {
    hash: u64,
    key: Vec<u8>,
    value: T,
}

enum Slot<T> {
    Empty,
    Deleted,
    Data(SlotData<T>),
}

impl<T> Slot<T> {
    fn insert(&mut self, data: SlotData<T>) -> &mut SlotData<T> {
        *self = Slot::Data(data);
        unsafe { &mut *Self::data_ptr(self) }
    }

    fn remove(&mut self) -> Option<SlotData<T>> {
        match mem::replace(self, Slot::Deleted) {
            Slot::Data(data) => Some(data),
            _ => None,
        }
    }

    unsafe fn data_ptr(ptr: *mut Slot<T>) -> *mut SlotData<T> {
        match &mut *ptr {
            Slot::Data(data) => data,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

pub struct StringMap<T> {
    bucket: Vec<Slot<T>>,
    len: usize,
}

impl<T> StringMap<T> {
    pub fn new() -> Self {
        Self::with_capacity(MIN_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let mut bucket = Vec::with_capacity(cap.max(MIN_CAPACITY));
        bucket.resize_with(cap, || Slot::<T>::Empty);
        StringMap { bucket, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<T> Default for StringMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Hash> StringMap<T> {
    pub fn get(&self, key: &[u8], hash: u64) -> Option<&T> {
        self.lookup(key, hash)
            .map(|ptr| unsafe { &(*Slot::data_ptr(ptr)).value })
    }

    pub fn get_mut(&mut self, key: &[u8], hash: u64) -> Option<&mut T> {
        self.lookup(key, hash)
            .map(|ptr| unsafe { &mut (*Slot::data_ptr(ptr)).value })
    }

    pub fn insert(&mut self, key: &[u8], hash: u64, value: T) -> Option<T> {
        let slot = self
            .lookup_or_free(key, hash)
            .expect("Failed to lookup slot");
        let old = mem::replace(
            unsafe { &mut *slot },
            Slot::Data(SlotData {
                key: key.to_owned(),
                hash,
                value,
            }),
        );
        self.len += 1;

        if self.len * LOAD_FACTOR_N / LOAD_FACTOR_D >= self.bucket.len() {
            self.resize(self.bucket.len() * 2);
        }

        match old {
            Slot::Data(data) => Some(data.value),
            _ => None,
        }
    }

    pub fn try_insert(&mut self, key: &[u8], hash: u64, value: T) -> Option<(&mut T, T)> {
        let mut trial = 0;
        while trial <= 1 {
            match self.lookup_or_free(key, hash) {
                Some(slot) => {
                    let slot = unsafe { &mut *slot };
                    match slot {
                        Slot::Empty | Slot::Deleted => {
                            slot.insert(SlotData {
                                key: key.to_owned(),
                                hash,
                                value,
                            });
                            self.len += 1;

                            if self.len * LOAD_FACTOR_N / LOAD_FACTOR_D >= self.bucket.len() {
                                self.resize(self.bucket.len() * 2);
                            }

                            return None;
                        }
                        Slot::Data(data) => return Some((&mut data.value, value)),
                    }
                }
                None => {
                    self.resize(self.bucket.len() * 2);
                    trial += 1
                }
            }
        }
        unreachable!("Failed to insert new key-value pair into the map")
    }

    pub fn remove(&mut self, key: &[u8], hash: u64) -> Option<T> {
        let slot = unsafe { &mut *self.lookup(key, hash)? };
        let ret = slot.remove()?;
        self.len -= 1;

        if self.len > MIN_CAPACITY
            && self.len * LOAD_FACTOR_N / LOAD_FACTOR_D <= self.bucket.len() / 2
        {
            self.resize(self.bucket.len() / 2);
        }

        Some(ret.value)
    }
}

impl<T: Hash> StringMap<T> {
    fn lookup(&self, key: &[u8], hash: u64) -> Option<*mut Slot<T>> {
        let len = self.bucket.len();
        for i in 0..len {
            let slot = &self.bucket[((hash as usize) + i) % len];
            match slot {
                Slot::Empty => return None,
                Slot::Data(data) if data.key == key => return Some(slot as *const _ as _),
                _ => {}
            }
        }
        None
    }

    fn lookup_or_free(&self, key: &[u8], hash: u64) -> Option<*mut Slot<T>> {
        let len = self.bucket.len();
        for i in 0..len {
            let slot = &self.bucket[((hash as usize) + i) % len];
            match slot {
                Slot::Empty | Slot::Deleted => return Some(slot as *const _ as _),
                Slot::Data(data) if data.key == key => return Some(slot as *const _ as _),
                _ => {}
            }
        }
        None
    }

    fn resize(&mut self, new_len: usize) {
        let mut bucket = Vec::with_capacity(new_len);
        bucket.resize_with(new_len, || Slot::<T>::Empty);
        let bucket = mem::replace(&mut self.bucket, bucket);
        for item in bucket {
            if let Slot::Data(data) = item {
                let slot = self.lookup_or_free(&data.key, data.hash).unwrap();
                unsafe { (*slot).insert(data) };
            }
        }
    }
}

impl<T> StringMap<T> {
    pub fn iter(&self) -> Iter<T> {
        Iter {
            bucket: &self.bucket,
            rem: self.len,
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<T> {
        IterMut {
            bucket: &mut self.bucket,
            rem: self.len,
        }
    }
}

impl<T> IntoIterator for StringMap<T> {
    type Item = (Vec<u8>, T);

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            bucket: self.bucket.into_iter(),
            rem: self.len,
        }
    }
}

impl<'a, T> IntoIterator for &'a StringMap<T> {
    type Item = (&'a [u8], &'a T);

    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut StringMap<T> {
    type Item = (&'a [u8], &'a mut T);

    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T: fmt::Debug> fmt::Debug for StringMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

pub struct Iter<'a, T> {
    bucket: &'a [Slot<T>],
    rem: usize,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (&'a [u8], &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.split_first() {
                Some((slot, rem)) => {
                    self.bucket = rem;
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some((&data.key, &data.value));
                    }
                }
                None => break None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem, Some(self.rem))
    }
}

impl<'a, T: fmt::Debug> fmt::Debug for Iter<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.clone()).finish()
    }
}

impl<'a, T> Clone for Iter<'a, T> {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket,
            rem: self.rem,
        }
    }
}

impl<'a, T> ExactSizeIterator for Iter<'a, T> {}
impl<'a, T> FusedIterator for Iter<'a, T> {}

pub struct IterMut<'a, T> {
    bucket: &'a mut [Slot<T>],
    rem: usize,
}

impl<'a, T> IterMut<'a, T> {
    fn iter(&self) -> Iter<T> {
        Iter {
            bucket: self.bucket,
            rem: self.rem,
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = (&'a [u8], &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match unsafe { &mut *(self.bucket as *mut [Slot<T>]) }.split_first_mut() {
                Some((slot, rem)) => {
                    self.bucket = rem;
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some((&data.key, &mut data.value));
                    }
                }
                None => break None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem, Some(self.rem))
    }
}

impl<'a, T: fmt::Debug> fmt::Debug for IterMut<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'a, T> ExactSizeIterator for IterMut<'a, T> {}
impl<'a, T> FusedIterator for IterMut<'a, T> {}

pub struct IntoIter<T> {
    bucket: std::vec::IntoIter<Slot<T>>,
    rem: usize,
}

impl<T> IntoIter<T> {
    fn iter(&self) -> Iter<T> {
        Iter {
            bucket: self.bucket.as_slice(),
            rem: self.rem,
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = (Vec<u8>, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.next() {
                Some(slot) => {
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some((data.key, data.value));
                    }
                }
                None => break None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem, Some(self.rem))
    }
}

impl<T: fmt::Debug> fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {}
impl<T> FusedIterator for IntoIter<T> {}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn test_hash_map() {
        let mut map = StringMap::new();
        let mut cmp = HashMap::new();
        let hasher = RandomState::default();

        for _ in 0..50000 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            map.insert(&key, hash, value);
            cmp.insert(key.to_vec(), value);
        }

        // Test conflict conditions
        for _ in 0..100000 {
            let value = rand::random::<u16>() as u64;
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            map.insert(&key, hash, value);
            cmp.insert(key.to_vec(), value);
        }

        for (k, v) in map {
            let value = cmp.remove(&k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }
}
