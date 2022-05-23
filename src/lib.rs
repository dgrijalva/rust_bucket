#[macro_use]
extern crate redis_module;

mod bucket;

use bucket::Bucket;
use redis_module::native_types::RedisType;
use redis_module::redisvalue::RedisValue;
use redis_module::{raw, Context, NextArg, RedisError, RedisResult, RedisString};
use std::os::raw::c_void;

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

        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,
    },
);

unsafe extern "C" fn free(value: *mut c_void) {
    Box::from_raw(value as *mut Bucket);
}

// - Redis Actions

fn bucket_create(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args
        .next()
        .ok_or_else(|| RedisError::Str("expected key name"))?;
    let bucket = Bucket::new(args.next_i64()?, args.next_i64()?);

    let key = ctx.open_key_writable(&key);
    key.set_value(&BUCKET_REDIS_TYPE, bucket)?;
    Ok(RedisValue::Integer(0))
}

fn bucket_take(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args
        .next()
        .ok_or_else(|| RedisError::Str("expected key name"))?;

    let key = ctx.open_key_writable(&key);
    let res = match key.get_value::<Bucket>(&BUCKET_REDIS_TYPE)? {
        Some(bucket) => bucket.take(1).map(|v| RedisValue::Integer(v)),
        None => Err(RedisError::nonexistent_key()),
    };

    res
}

fn bucket_peek(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args
        .next()
        .ok_or_else(|| RedisError::Str("expected key name"))?;

    let key = ctx.open_key(&key);
    match key.get_value::<Bucket>(&BUCKET_REDIS_TYPE)? {
        Some(bucket) => bucket.peek().map(|v| RedisValue::Integer(v)),
        None => Ok(RedisValue::Null),
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
