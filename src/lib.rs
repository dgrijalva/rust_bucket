#[macro_use]
extern crate redis_module;

mod bucket;

use bucket::{Bucket, BUCKET_REDIS_TYPE};
use redis_module::redisvalue::RedisValue;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString};

// - Redis Actions

/// Create a new bucket. `bucket.create :key :capacity :ms_per_token`
fn bucket_create(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key_name = args
        .next()
        .ok_or_else(|| RedisError::Str("expected key name"))?;
    let bucket = Bucket::new(args.next_i64()?, args.next_i64()?)?;

    let key = ctx.open_key_writable(&key_name);
    // log_notice(&format!("Created {:?} : {:?}", key_name, bucket));
    key.set_value(&BUCKET_REDIS_TYPE, bucket)?;
    Ok(RedisValue::Integer(0))
}

/// Take tickets from bucket. `bucket.take :key [:quantity (default 1)]`
fn bucket_take(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key_name = args
        .next()
        .ok_or_else(|| RedisError::Str("expected key name"))?;

    // optional second argument for how many tokens to take
    let mut amount = 1i64;
    if let Some(v) = args.next().map(|s| s.parse_integer().ok()).flatten() {
        amount = v;
    }

    let key = ctx.open_key_writable(&key_name);
    match key.get_value::<Bucket>(&BUCKET_REDIS_TYPE)? {
        Some(bucket) => {
            // log_notice(&format!("Read {:?} : {:?}", key_name, bucket));
            let v = bucket.take(amount).map(|v| RedisValue::Integer(v))?;
            // log_notice(&format!("Take: {:?} | Post value: {:?}", v, bucket));
            Ok(v)
        }
        None => Err(RedisError::nonexistent_key()),
    }
}

/// Get number of tokens in bucket. `bucket.peek :key`
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
