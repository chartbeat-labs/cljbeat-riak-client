(ns riak-client.json
  (:require [riak-client.core :refer [fetch
                                      fetch-async
                                      ffetch
                                      ffetch-async
                                      store
                                      store-async
                                      riak-object]]
            [riak-client.utils :refer [wrap-future]]
            [clojure.data.json :as json]))


(defn fetch-json-async
  "Wrap function for fetching and reading json as collections. Returns a future.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch-async
  - json-read-str-options, map: options for json/read-str"
  ([client loc]
   (fetch-json-async client loc {} {}))
  ([client loc fetch-options]
   (fetch-json-async client loc fetch-options {}))
  ([client loc fetch-options json-read-str-options]
   (wrap-future (fetch-async client loc fetch-options)
                (fn [siblings]
                  (map #(apply json/read-str % json-read-str-options)
                       siblings)))))


(defn fetch-json
  "Wrap function for fetching and reading json as collections.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch
  - json-read-str-options, map: options for json/read-str"
  ([client loc]
   (fetch-json client loc {} {}))
  ([client loc fetch-options]
   (fetch-json client loc fetch-options {}))
  ([client loc fetch-options json-read-str-options]
   (map #(apply json/read-str % json-read-str-options)
        (fetch client loc fetch-options))))


(defn ffetch-json-async
  "Wrap function for fetching and reading json as collections. Returns a future.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch-async
  - json-read-str-options, map: options for json/read-str"
  ([client loc]
   (ffetch-json-async client loc {} {}))
  ([client loc fetch-options]
   (ffetch-json-async client loc fetch-options {}))
  ([client loc fetch-options json-read-str-options]
   (wrap-future (ffetch-async client loc fetch-options)
                #(apply json/read-str % json-read-str-options))))


(defn ffetch-json
  "Wrap function for fetching and reading json as collections.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch
  - json-read-str-options, map: options for json/read-str"
  ([client loc]
   (ffetch-json client loc {} {}))
  ([client loc fetch-options]
   (ffetch-json client loc fetch-options {}))
  ([client loc fetch-options json-read-str-options]
   (apply json/read-str
          (ffetch client loc fetch-options)
          json-read-str-options)))


(defn store-json-async
  "Wrap function for storing and writing collections as json. Returns a riak
  future.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - store-options, map: options for riak-client/store
  - json-write-str-options, map: options for json/write-str"
  ([client loc m]
   (store-json-async client loc m {} {}))
  ([client loc m store-options]
   (store-json-async client loc m store-options {}))
  ([client loc m store-options json-write-str-options]
   (let [jstr (apply json/write-str m json-write-str-options)
         robj (riak-object jstr "application/json")]
     (store-async client loc robj store-options))))


(defn store-json
  "Wrap function for storing and writing collections as json.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - store-options, map: options for riak-client/store
  - json-write-str-options, map: options for json/write-str"
  ([client loc m]
   (store-json client loc m {} {}))
  ([client loc m store-options]
   (store-json client loc m store-options {}))
  ([client loc m store-options json-write-str-options]
   (let [jstr (apply json/write-str m json-write-str-options)
         robj (riak-object jstr "application/json")]
     (store client loc robj store-options))))

