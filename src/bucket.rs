use redis_module::redisraw::bindings::RedisModule_Milliseconds;
use redis_module::RedisError;
use std::cmp::min;

// Bucket format: [value: i64, capacity: i64, fill_rate: i64, last_fill: i64]
// value: number of tokens in bucket
// capacity: max number of tokens in bucket
// fill_rate: add 1 token every fill_rate ms
// last_fill: unix timestamp of last time fill operation was run, in ms

#[derive(Debug)]
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
            capacity: capacity,
            fill_rate: fill_rate,
            last_fill: Self::time()?,
        })
    }

    pub fn take(&mut self, tokens: i64) -> Result<i64, RedisError> {
        if tokens < 1 {
            return Err(RedisError::Str("'tokens' must be 1 or greater"));
        }

        let (new_value, t) = self.new_value(Self::time()?);
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

    fn new_value(&self, time: i64) -> (i64, i64) {
        let additions = (time - self.last_fill) / self.fill_rate;
        let last_fill = self.last_fill + (additions * self.fill_rate);
        (min(self.value + additions, self.capacity), last_fill)
    }
}
