use std::fmt;
use std::iter::{self, FusedIterator};

pub struct StringMap<T> {
    data: Vec<Option<T>>,
    len: usize,
}

fn to_index(key: &[u8]) -> usize {
    u16::from_ne_bytes(key.try_into().unwrap()) as usize
}

fn from_index(index: usize) -> [u8; 2] {
    (index as u16).to_ne_bytes()
}

impl<T> StringMap<T> {
    pub fn new() -> Self {
        StringMap {
            data: iter::repeat_with(|| None).take(65536).collect(),
            len: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, key: &[u8]) -> Option<&T> {
        self.data.get(to_index(key)).and_then(|slot| slot.as_ref())
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        self.data
            .get_mut(to_index(key))
            .and_then(|slot| slot.as_mut())
    }

    pub fn insert(&mut self, key: &[u8], value: T) -> Option<T> {
        let ret = self.data.get_mut(to_index(key))?.replace(value);
        if ret.is_none() {
            self.len += 1;
        }
        ret
    }

    pub fn try_insert(&mut self, key: &[u8], value: T) -> Option<(&mut T, T)> {
        let slot = self.data.get_mut(to_index(key))?;
        match slot {
            Some(s) => Some((s, value)),
            None => {
                *slot = Some(value);
                self.len += 1;
                None
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<T> {
        let ret = self.data.get_mut(to_index(key))?.take();
        if ret.is_some() {
            self.len -= 1;
        }
        ret
    }

    pub fn iter(&self) -> Iter<T> {
        Iter {
            bucket: &self.data,
            index: 0,
            rem: self.len,
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<T> {
        IterMut {
            bucket: &mut self.data,
            index: 0,
            rem: self.len,
        }
    }
}

impl<T> Default for StringMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IntoIterator for StringMap<T> {
    type Item = ([u8; 2], T);

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            bucket: self.data.into_iter(),
            index: 0,
            rem: self.len,
        }
    }
}

pub struct Iter<'a, T> {
    bucket: &'a [Option<T>],
    index: usize,
    rem: usize,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = ([u8; 2], &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.split_first() {
                Some((slot, rem)) => {
                    let index = self.index;

                    self.bucket = rem;
                    self.index += 1;

                    if let Some(data) = slot {
                        self.rem -= 1;
                        break Some((from_index(index), data));
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
            index: self.index,
            rem: self.rem,
        }
    }
}

impl<'a, T> ExactSizeIterator for Iter<'a, T> {}
impl<'a, T> FusedIterator for Iter<'a, T> {}

pub struct IterMut<'a, T> {
    bucket: &'a mut [Option<T>],
    index: usize,
    rem: usize,
}

impl<'a, T> IterMut<'a, T> {
    fn iter(&self) -> Iter<T> {
        Iter {
            bucket: self.bucket,
            index: self.index,
            rem: self.rem,
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = ([u8; 2], &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match unsafe { &mut *(self.bucket as *mut [Option<T>]) }.split_first_mut() {
                Some((slot, rem)) => {
                    let index = self.index;

                    self.bucket = rem;
                    self.index += 1;

                    if let Some(data) = slot {
                        self.rem -= 1;
                        break Some((from_index(index), data));
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
    bucket: std::vec::IntoIter<Option<T>>,
    index: usize,
    rem: usize,
}

impl<T> IntoIter<T> {
    fn iter(&self) -> Iter<T> {
        Iter {
            bucket: self.bucket.as_slice(),
            index: self.index,
            rem: self.rem,
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = ([u8; 2], T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.next() {
                Some(slot) => {
                    let index = self.index;

                    self.index += 1;

                    if let Some(data) = slot {
                        self.rem -= 1;
                        break Some((from_index(index), data));
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

impl<'a, T: fmt::Debug> fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {}
impl<T> FusedIterator for IntoIter<T> {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_hash_map() {
        let mut map = StringMap::new();
        let mut cmp = HashMap::new();

        // Test conflict conditions
        for _ in 0..100000 {
            let value = rand::random::<u16>();
            let key = value.to_ne_bytes();

            map.insert(&key, value);
            cmp.insert(key, value);
        }

        for (k, v) in map {
            let value = cmp.remove(&k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }
}
