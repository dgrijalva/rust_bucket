# rust_bucket
[Leaky bucket throttling](https://en.wikipedia.org/wiki/Leaky_bucket) as a redis module, implemented in rust (mostly for learning)

I pretty much just wrote this to mess around with FFI and the redis module API. It turns out, someone already published a pretty nice rust crate for building redis modules (thanks!). 

## Using this

`cargo build --release` should produce a shared library that can be directly loaded into redis using the [`MODULE LOAD`](https://redis.io/docs/reference/modules/) command.

`bucket.create foo 1000 1` will create a bucket at the key `foo` that holds 1000 tokens and adds 1 token every 1ms.

`bucket.take foo` will attempt to take one token from the bucket, returning either 0 or 1.

`bucket.take foo 10` will attempt to take 10 tokens from the bucket, returning either 0 or 10.

`bucket.peek foo` will return the number of tokens currently in the bucket.


## TODO

* Add support for adding more than one token per ms. How should it be specified? Extra argument? Negative add_rate?
