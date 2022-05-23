use redis_module::logging::{log_debug, log_notice, log_warning};
use redis_module::redisraw::bindings::RedisModule_Milliseconds;
use redis_module::{raw, Context, NextArg, RedisError, RedisResult, RedisString};
use std::cmp::min;
use std::os::raw::{c_int, c_void};

// Bucket format: [value: i64, capacity: i64, fill_rate: i64, last_fill: i64]
// value: number of tokens in bucket
// capacity: max number of tokens in bucket
// fill_rate: add 1 token every fill_rate ms
// last_fill: unix timestamp of last time fill operation was run, in ms

#[derive(Debug, Clone, Copy)]
pub struct Bucket {
    value: i64,
    capacity: i64,
    fill_rate: i64,
    last_fill: i64,
}

impl Bucket {
    pub fn new(capacity: i64, fill_rate: i64) -> Result<Self, RedisError> {
        Ok(Self {
            value: 0,
            capacity,
            fill_rate,
            last_fill: Self::time()?,
        })
    }

    pub fn take(&mut self, tokens: i64) -> Result<i64, RedisError> {
        if tokens < 1 {
            return Err(RedisError::Str("'tokens' must be 1 or greater"));
        }

        let time = Self::time()?;
        log_notice(&format!(
            "Old time: {:?} New Time: {:?}",
            self.last_fill, time
        ));
        let (new_value, t) = self.new_value(time);
        if new_value > tokens {
            self.value = new_value - tokens;
            self.last_fill = t;
            // key.set_value(&BUCKET_REDIS_TYPE, bucket)?;
            Ok(tokens)
        } else {
            Ok(0)
        }
    }

    pub fn peek(&self) -> Result<i64, RedisError> {
        let (v, _) = self.new_value(Self::time()?);
        Ok(v)
    }

    fn time() -> Result<i64, RedisError> {
        // FIXME: can we get at this without unsafe?
        unsafe {
            if let Some(f) = RedisModule_Milliseconds {
                Ok(f())
            } else {
                Err(RedisError::Str(
                    "RedisMillisecond is required but not available",
                ))
            }
        }
    }

    /// Calculates the new value given the specified time
    /// Returns the new value and the time of the last addition
    fn new_value(&self, time: i64) -> (i64, i64) {
        // How long has passed
        let elapsed_ms = if time > self.last_fill {
            time - self.last_fill
        } else {
            0
        };

        // How many tokens should we have added
        let additions = elapsed_ms / self.fill_rate;

        // How many ms would have passed to have added that many tokens
        // This is different from `time` because time may be between additions,
        // which would have the side effect of slowing down additions if there's
        // a high `take` rate.
        let last_fill = self.last_fill + (additions * self.fill_rate);

        // Apply additions, limited by capacity
        (min(self.value + additions, self.capacity), last_fill)
    }
}

pub extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
    let load = || -> Result<Bucket, RedisError> {
        Ok(Bucket {
            value: raw::load_signed(rdb)?,
            capacity: raw::load_signed(rdb)?,
            fill_rate: raw::load_signed(rdb)?,
            last_fill: raw::load_signed(rdb)?,
        })
    };

    match load() {
        Ok(bucket) => Box::into_raw(Box::new(bucket)) as *mut c_void,
        Err(err) => {
            log_notice(&format!("Error reading bucket value {:?}", err));
            0 as *mut c_void
        }
    }
}

pub unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let bucket = &*(value as *mut Bucket);
    raw::save_signed(rdb, bucket.value);
    raw::save_signed(rdb, bucket.capacity);
    raw::save_signed(rdb, bucket.fill_rate);
    raw::save_signed(rdb, bucket.last_fill);
}

pub unsafe extern "C" fn free(value: *mut c_void) {
    Box::from_raw(value as *mut Bucket);
}

// FIXME: tests don't run for some reason

// #[cfg(test)]
// mod test {
//     use super::*;

//     struct TestCase {
//         name: &'static str,
//         capacity: i64,
//         fill_rate: i64,
//         last_fill: i64,
//         elapsed: i64,
//         result: i64,
//     }

//     #[test]
//     fn test_new_value() {
//         let cases = [TestCase {
//             name: "zero",
//             capacity: 0,
//             fill_rate: 0,
//             last_fill: 0,
//             elapsed: 0,
//             result: 0,
//         }];

//         for case in cases {
//             let bucket = Bucket {
//                 value: 0,
//                 capacity: case.capacity,
//                 fill_rate: case.fill_rate,
//                 last_fill: case.last_fill,
//             };

//             let (result, _) = bucket.new_value(case.last_fill + case.elapsed);
//             assert_eq!(case.result, result);
//         }
//     }
// }
