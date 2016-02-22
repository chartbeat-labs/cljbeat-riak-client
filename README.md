# riak-client

A light clojure wrapper around the official Basho Java Riak 2.x client library.

## Usage

### Basics

**Connecting**
```clojure
user=> (require '[riak-client.core :as riak-client])
nil
user=> (def client (riak-client/connect ["host01.sample.com" "host02.sample.com" "host03.sample.com"]))
#'user/client
```

**Storing**
```clojure
user=> (riak-client/store client ["foo" "bar"] "hello world")
#object[com.basho.riak.client.api.commands.kv.StoreValue$Response 0x3000447e "{location: {namespace: {type: default, bucket: foo}, key: bar}, values: []}"]
```

**Fetching**
```clojure
user=> (riak-client/fetch client ["foo" "bar"])
("hello world")
```

**Deleting**
```clojure
user=> (riak-client/delete client ["foo" "bar"])
nil
```

### Locations

Locations in the Java API are Location objects constructed by a Namespace of the 
bucket type and bucket and a BinaryValue key. In our clojure API, this is all
handled by passing a vector of two or three strings. Two strings represent a
bucket and key in the default bucket type (similar to riak 1.4.x). Three strings
represent a bucket type, bucket and key.

See the private function `riak-client/parse-loc-vec` for details.

```clojure
user=> (#'riak-client/parse-loc-vec ["herp" "derp"])
#object[com.basho.riak.client.core.query.Location 0x50a48e29 "{namespace: {type: default, bucket: herp}, key: derp}"]
user=> (#'riak-client/parse-loc-vec ["qux" "herp" "derp"])
#object[com.basho.riak.client.core.query.Location 0x516ca402 "{namespace: {type: qux, bucket: herp}, key: derp}"]
```


### Conflict Resolution

This client library encourages proper conflict resolution. Conflicts are the
result of two separate riak nodes being updated in different ways 
simultaneously. These two different values are called siblings. To account for
this, `fetch` and the future returned by `fetch-async` always return a list of
siblings.

If you are certain that you won't have conflicts, or simply don't care, you can
use the convenience functions `ffetch` and `ffetch-async` which are equivalent
to calling `(first (fetch ...))`.


### Aysnc

The Java Riak API exposes an async API which returns futures. Under the hood, in
both the Java API and this one, all sync operations are actually async. The
async versions of `fetch`, `ffetch`, `store`, and `delete` are exposed as
`fetch-async`, `ffetch-async`, `store-async`, `delete-async` respectively.


## License
### JSON

A light JSON wrapper using clojure.data.json is exposed via `riak-client.json`.
```clojure
user=> (require '[riak-client.json :as json-client])
nil
user=> (json-client/store-json conn ["foo" "bar"] {:a 1 :b 2 :c [1 2 3]})
#object[com.basho.riak.client.api.commands.kv.StoreValue$Response 0x31167838 "{location: {namespace: {type: default, bucket: foo}, key: bar}, values: []}"]
user=> (json-client/ffetch-json conn ["foo" "bar"])
{"a" 1 "b" 2 "c" [1 2 3]}
```
Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
