(ns riak-client.edn
  (:require [riak-client.core :refer [fetch
                                      fetch-async
                                      ffetch
                                      ffetch-async
                                      store
                                      store-async
                                      riak-object]]
            [riak-client.utils :refer [wrap-future]]
            [clojure.edn :as edn]))


(defn fetch-edn-async
  "WARNING: Experimental!!!

  Wrap function for fetching and reading clojure collections as edn. Returns a
  future.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch"
  ([client loc]
   (fetch-edn-async client loc {}))
  ([client loc fetch-options]
   (wrap-future (fetch-async client loc fetch-options)
                #(map edn/read-string %))))


(defn fetch-edn
  "WARNING: Experimental!!!

  Wrap function for fetching and reading clojure collections as edn.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch"
  ([client loc]
   (fetch-edn client loc {}))
  ([client loc fetch-options]
   (map edn/read-string (fetch client loc fetch-options))))


(defn ffetch-edn-async
  "WARNING: Experimental!!!

  Wrap function for fetching and reading clojure collections as edn. Returns a
  future.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch"
  ([client loc]
   (ffetch-edn-async client loc {}))
  ([client loc fetch-options]
   (wrap-future (ffetch-async client loc fetch-options)
                edn/read-string)))


(defn ffetch-edn
  "WARNING: Experimental!!!

  Wrap function for fetching and reading clojure collections as edn.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - fetch-options, map: options for riak-client/fetch"
  ([client loc]
   (ffetch-edn client loc {}))
  ([client loc fetch-options]
   (edn/read-string (ffetch client loc fetch-options))))


(defn store-edn-async
  "WARNING: Experimental!!!

  Wrap function for storing and writing collections as edn. Returns a riak
  future.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - store-options, map: options for riak-client/store"
  ([client loc coll]
   (store-edn-async client loc coll {}))
  ([client loc coll store-options]
   (let [edn-str (pr-str coll)
         robj (riak-object edn-str "application/edn")]
     (store-async client loc robj store-options))))


(defn store-edn
  "WARNING: Experimental!!!

  Wrap function for storing and writing collections as edn.

  Required:
  - client, RiakClient: see riak-client/connect
  - loc, vector: see riak-client/parse-loc-vector

  Optional:
  - store-options, map: options for riak-client/store"
  ([client loc coll]
   (store-edn client loc coll {}))
  ([client loc coll store-options]
   (let [edn-str (pr-str coll)
         robj (riak-object edn-str "application/edn")]
     (store client loc robj store-options))))
