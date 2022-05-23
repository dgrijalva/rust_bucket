#[macro_use]
extern crate redis_module;

use redis_module::{raw, Context, NextArg, RedisResult, RedisError};
use redis_module::redisvalue::RedisValue;
use std::cmp::min;
use byteorder::{BigEndian,ByteOrder};
use redis_module::native_types::RedisType;
use redis_module::redisraw::bindings::{RedisModule_Milliseconds, RedisModule_SaveStringBuffer, RedisModule_LoadStringBuffer};
use std::os::raw::{c_void, c_int, c_char};


// Bucket format: [value: i64, capacity: i64, fill_rate: i64, last_fill: i64]
// value: number of tokens in bucket
// capacity: max number of tokens in bucket
// fill_rate: add 1 token every fill_rate ms
// last_fill: unix timestamp of last time fill operation was run, in ms

#[derive(Debug)]
struct Bucket {
    value: i64,
    capacity: i64,
    fill_rate: i64,
    last_fill: i64
}

impl Bucket {
    fn new_value(&self, time: i64) -> (i64, i64) {
        let additions = (time - self.last_fill) / self.fill_rate;
        let last_fill = self.last_fill + (additions * self.fill_rate);
        (min(self.value + additions, self.capacity), last_fill)
    }

    fn to_buf(&self) -> Vec<u8> {
        let mut buf = vec!(0;32);
        BigEndian::write_i64(&mut buf[..7], self.value);
        BigEndian::write_i64(&mut buf[8..15], self.capacity);
        BigEndian::write_i64(&mut buf[16..23], self.fill_rate);
        BigEndian::write_i64(&mut buf[24..], self.last_fill);
        buf
    }

    fn from_buf(value: &[u8]) -> Bucket {
        Bucket{
            value: BigEndian::read_i64(&value[..7]), 
            capacity: BigEndian::read_i64(&value[8..15]), 
            fill_rate: BigEndian::read_i64(&value[16..23]), 
            last_fill: BigEndian::read_i64(&value[24..])
        }
    }
}

static BUCKET_REDIS_TYPE: RedisType = RedisType::new(
    "dg-Bucket",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        // rdb_load: Some(rdb_load),
        // rdb_save: Some(rdb_save),
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None,
        free: Some(free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,

        // Aux data
        aux_load: None,
        aux_save: None,
        aux_save_triggers: 0,
    },
);

unsafe extern "C" fn free(value: *mut c_void) {
    Box::from_raw(value as *mut Bucket);
}

extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
    let mut len = 0;
    let buf : &[u8];
    unsafe {
        let c_buffer = RedisModule_LoadStringBuffer.unwrap()(rdb, &mut len);
        // let buf : &[u8] = &*(c_buffer as *mut [u8;len]);
        buf = std::slice::from_raw_parts(c_buffer as *const u8, len);
    }
    let bucket = Bucket::from_buf(buf);
    Box::into_raw(Box::new(bucket)) as *mut c_void
}

unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let bucket = &*(value as *mut Bucket);
    let buf = bucket.to_buf();
    RedisModule_SaveStringBuffer.unwrap()(rdb, buf.as_ptr() as *const c_char, buf.len())
}

// impl Bucket {
//     fn unpack_int(value: RedisValue) -> Result<i64, &'static str> {
//         if let RedisValue::Integer(v) = value {
//             return Ok(v)
//         }
//         Err(UNPACK_ERROR)
//     }
// }

// const UNPACK_ERROR : &str = "Value is not a bucket";
const TIME_ERROR : &str = "RedisMillisecond is required but not available";
// impl TryFrom<&str> for Bucket {
//     type Error = &'static str;

//     fn try_from(value: &str) -> Result<Self, Self::Error> {
//         if value.len() == 32 {
//             Ok(Bucket{
//                 value: BigEndian::read_i64(&value[..7].as_bytes()), 
//                 capacity: BigEndian::read_i64(&value[8..15].as_bytes()), 
//                 fill_rate: BigEndian::read_i64(&value[16..23].as_bytes()), 
//                 last_fill: BigEndian::read_i64(&value[24..].as_bytes())
//             })
//         } else {
//             Err(UNPACK_ERROR)
//         }
//     }
// }

fn time() -> Result<i64,RedisError> {
    // FIXME: can we get at this without unsafe?
    unsafe {
        if let Some(f) = RedisModule_Milliseconds {
            Ok(f())
        } else {
            Err(RedisError::Str(TIME_ERROR))
        }    
    }
}

fn bucket_create(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;
    let bucket = Bucket{
        value: 0, 
        capacity: args.next_i64()?, 
        fill_rate: args.next_i64()?, 
        last_fill: time()?,
    };

    let key = ctx.open_key_writable(&key);
    key.set_value(&BUCKET_REDIS_TYPE, bucket)?;
    Ok(RedisValue::Integer(0))
}

fn bucket_take(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;

    let key = ctx.open_key_writable(&key);
    let res = match key.get_value::<Bucket>(&BUCKET_REDIS_TYPE)? {
        Some(bucket) => {
            let (new_value, t) = bucket.new_value(time()?);
            if new_value > 1 {
                bucket.value = new_value - 1;
                bucket.last_fill = t;
                // key.set_value(&BUCKET_REDIS_TYPE, bucket)?;
                Ok(RedisValue::Integer(1))
            } else {
                Ok(RedisValue::Integer(0))
            }
        }
        None => Err(RedisError::nonexistent_key())
    };

    res
}

fn bucket_peek(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;

    let key = ctx.open_key(&key);
    match key.get_value::<Bucket>(&BUCKET_REDIS_TYPE)? {
        Some(bucket) => {
            let (v, _) = bucket.new_value(time()?);
            Ok(RedisValue::Integer(v))
        }
        None => Ok(RedisValue::Null)
    }
}

//////////////////////////////////////////////////////

redis_module! {
    name: "bucket",
    version: 1,
    data_types: [BUCKET_REDIS_TYPE],
    commands: [
        ["bucket.create", bucket_create, "write", 1, 1, 1],
        ["bucket.take", bucket_take, "write", 1, 1, 1],
        ["bucket.peek", bucket_peek, "readonly", 1, 1, 1],
    ],
}