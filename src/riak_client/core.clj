(ns riak-client.core
  (:require [riak-client.utils :refer [wrap-future]])
  (:import [clojure.lang Keyword Symbol]
           [com.basho.riak.client.api RiakClient]
           [com.basho.riak.client.api.cap Quorum]
           [com.basho.riak.client.api.commands.kv FetchValue$Builder
                                                  FetchValue$Option
                                                  StoreValue$Builder
                                                  StoreValue$Option
                                                  DeleteValue$Builder
                                                  DeleteValue$Option]
           [com.basho.riak.client.core.util BinaryValue]
           [com.basho.riak.client.core.query Namespace Location]
           [com.basho.riak.client.core.query RiakObject]
           [com.basho.riak.client.core.query.crdt.types RiakCounter
                                                        RiakFlag
                                                        RiakSet
                                                        RiakRegister
                                                        RiakMap]))


;; abstracted for easier testing
(defn- execute-async [client cmd]
  (.executeAsync client cmd))


;; abstracted for easier testing
(defn- get-values
  [response]
  (.getValues response RiakObject))


(defn- ^BinaryValue binary-value
  "Returns a riak BinaryValue of x when x is a byte-array, string, symbol,
  keyword or int."
  [x]
  (let [ByteArray (Class/forName "[B")
        int-to-byte-array (fn [i]
                            (.. (java.nio.ByteBuffer/allocate 4)
                                (putInt (int 42))
                                (array)))]
    (condp instance? x
      Keyword    (BinaryValue/create (name x))
      Symbol     (BinaryValue/create (name x))
      String     (BinaryValue/create x)
      ByteArray  (BinaryValue/create x)
      Integer    (BinaryValue/create (int-to-byte-array x))
      (throw (Exception. (format "Can not create BinaryValue from %s"
                                 (type x)))))))


(defn ^RiakObject riak-object
  "Creates a RiakObject with optional content-type."
  ([value]
   (condp instance? value
     ;; if it's a RiakObject already, don't do a thing
     RiakObject value
     ;; if it's a String, set the contentType so it comes back out as a string
     java.lang.String (riak-object value "text/plain")
     ;; if it's Serializable (like all core clojure data structures are), serialize it
     java.io.Serializable

     ;; otherwise, just use the default contentType
     (riak-object value RiakObject/DEFAULT_CONTENT_TYPE)))
  ([value content-type]
   (..
     ;; only create a RiakObject if it's no one already
     (if (instance? RiakObject value)
       value
       (.. (RiakObject.)
           (setValue (binary-value value))))
     (setContentType content-type))))


(defn- to-clojure
  "Converts one of the many riak-specific java classes into a palatable clojure
  object."
  [obj & flags]
  (let [bytes-to-string ((set flags) :bytes-to-string)]
    ((fn -to-clojure
       [obj]
       (condp instance? obj
         BinaryValue  (if bytes-to-string (.toString obj) (.getValue obj))
         RiakObject   (condp = (.getContentType obj)
                        "text/plain"       (.toString (.getValue obj))
                        "application/json" (.toString (.getValue obj))
                        "application/edn"  (.toString (.getValue obj))
                        (-to-clojure (.getValue obj)))
         RiakObject   (-to-clojure (.getValue obj))
         RiakCounter  (.view obj)
         RiakFlag     (.view obj)
         RiakRegister (-to-clojure (.view obj))
         RiakSet      (set (map -to-clojure (.view obj)))
         RiakMap      (zipmap (map -to-clojure (.keySet (.view obj)))
                              (map (comp -to-clojure first) (.values (.view obj))))
         (throw (ex-info "Unknown type of object in to-clojure"
                         {:type (type obj) :obj obj}))))
     obj)))


(defn- parse-loc-vec
  "Returns a Location object for the bucket-type, bucket, and key which are
  passed in a vector.

  - If loc-vec is of count 2, elements represent bucket and key.
  - If loc-vec is of count 3, elements represent bucket type, bucket and key.

  When bucket-type is not set, \"default\" is used instead."
  [loc-vec]
  (apply
    (fn
      ([bucket-type bucket key]
        (Location. (Namespace. bucket-type bucket) (binary-value key)))
      ([bucket key]
        (Location. (Namespace. bucket) (binary-value key))))
    loc-vec))


(defn connect
  "Create a new RiakClient connection to hosts passed as a sequential."
  [hosts]
  (RiakClient/newClient hosts))


(def ^:private FETCH_OPTIONS_MAP
  "Mapping from keywords to underlying FetchValue options.
  see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/FetchValue.Option.html"
  {:R              FetchValue$Option/R
   :PR             FetchValue$Option/PR
   :BASIC_QUORUM   FetchValue$Option/BASIC_QUORUM
   :NOTFOUND_OK    FetchValue$Option/NOTFOUND_OK
   :IF_MODIFIED    FetchValue$Option/IF_MODIFIED
   :HEAD           FetchValue$Option/HEAD
   :DELETED_VCLOCK FetchValue$Option/DELETED_VCLOCK
   :TIMEOUT        FetchValue$Option/TIMEOUT
   :SLOPPY_QUORUM  FetchValue$Option/SLOPPY_QUORUM
   :N_VAL          FetchValue$Option/N_VAL})


(defn fetch-async
  "Asynchronous fetch from riak. Returns a future that when derefed will contain
  a sequence of resulting siblings from the request.

  Required:
    - client, RiakClient: see connect
    - loc, vector: see parse-loc-vector

  Optional:
    - options, map: exposes underlying FetchValue options via a map of keywords.
      see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/FetchValue.Option.html"
  ([client loc options]
   (let [builder (FetchValue$Builder. (parse-loc-vec loc))
         builder (reduce (fn [b [k v]] (.withOption b (FETCH_OPTIONS_MAP k) v))
                         builder options)
         cmd (.build builder)
         fut (execute-async client cmd)]
     (wrap-future fut #(map to-clojure (get-values %)))))
  ([client loc]
   (fetch-async client loc {})))


(defn fetch
  "Synchronous fetch from riak. Returns a sequence of resulting siblings from
  the request.

  Required:
    - client, RiakClient: see connect
    - loc, vector: see parse-loc-vector

  Optional:
    - options, map: exposes underlying FetchValue options via a map of keywords.
      see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/FetchValue.Option.html"
  ([client loc options]
   @(fetch-async client loc options))
  ([client loc]
   (fetch client loc {})))


(defn ffetch-async
  "Same as (future (first @(fetch-async ...))). To be used when you are sure
  there are no sibling conflicts."
  ([client loc options]
   (wrap-future (fetch-async client loc options) first))
  ([client loc]
   (ffetch-async client loc {})))


(defn ffetch
  "Same as (first (fetch ...)). To be used when you are sure there are no
  sibling conflicts."
  ([client loc options]
   (first (fetch client loc options)))
  ([client loc]
   (ffetch client loc {})))


(def ^:private STORE_OPTIONS_MAP
  "Mapping from keywords to underlying StoreValue options.
  see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/FetchValue.Option.html"
  {:W               StoreValue$Option/W
   :DW              StoreValue$Option/DW
   :PW              StoreValue$Option/PW
   :IF_NOT_MODIFIED StoreValue$Option/IF_NOT_MODIFIED
   :IF_NONE_MATCH   StoreValue$Option/IF_NONE_MATCH
   :RETURN_BODY     StoreValue$Option/RETURN_BODY
   :RETURN_HEAD     StoreValue$Option/RETURN_HEAD
   :TIMEOUT         StoreValue$Option/TIMEOUT
   :ASIS            StoreValue$Option/ASIS
   :SLOPPY_QUORUM   StoreValue$Option/SLOPPY_QUORUM
   :N_VAL           StoreValue$Option/N_VAL})


(defn store-async
  "Stores binary value or RiakObject at location loc. Returns a future.

  Required:
    - client, RiakClient: see connect
    - loc, vector: see parse-loc-vector
    - store-value, binary OR RiakObject (to make, see: riak-object)

  Optional:
    - options, map: exposes underlying StoreValue options via a map of keywords.
    see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/StoreValue.Option.html"
  ([client loc store-value options]
   (let [riak-obj (riak-object store-value)
         builder (StoreValue$Builder. riak-obj)
         builder (.withLocation builder (parse-loc-vec loc))
         builder (reduce (fn [b [k v]] (.withOption b (STORE_OPTIONS_MAP k) v))
                         builder options)
         cmd (.build builder)]
     (execute-async client cmd)))
  ([client loc store-value]
   (store-async client loc store-value {})))


(defn store
  "Stores binary value or RiakObject at location loc.

  Required:
    - client, RiakClient: see connect
    - loc, vector: see parse-loc-vector
    - store-value, binary OR RiakObject

  Optional:
    - options, map: exposes underlying StoreValue options via a map of keywords.
    see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/StoreValue.Option.html"
  ([client loc store-value options]
   @(store-async client loc store-value options))
  ([client loc store-value]
   (store client loc store-value {})))


(def ^:private DELETE_OPTIONS_MAP
  "Mapping from keywords to underlying DeleteValue options.
  see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/DeleteValue.Option.html"
  {:RW            DeleteValue$Option/RW
   :R             DeleteValue$Option/R
   :W             DeleteValue$Option/W
   :PR            DeleteValue$Option/PR
   :PW            DeleteValue$Option/PW
   :DW            DeleteValue$Option/DW
   :TIMEOUT       DeleteValue$Option/TIMEOUT
   :SLOPPY_QUORUM DeleteValue$Option/SLOPPY_QUORUM
   :N_VAL         DeleteValue$Option/N_VAL})


(defn delete-async
  "Deletes value at location loc. Returns a future.

  Required:
  - client, RiakClient: see connect
  - loc, vector: see parse-loc-vector

  Optional:
    - options, map: exposes underlying DeleteValue options via a map of keywords.
    see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/DeleteValue.Option.html"
  ([client loc options]
   (let [builder (DeleteValue$Builder. (parse-loc-vec loc))
         builder (reduce (fn [b [k v]] (.withOption b (DELETE_OPTIONS_MAP k) v))
                         builder options)
         cmd (.build builder)]
     (execute-async client cmd)))
  ([client loc]
   (delete-async client loc {})))


(defn delete
  "Deletes value at location loc. Returns a future.

  Required:
  - client, RiakClient: see connect
  - loc, vector: see parse-loc-vector

  Optional:
    - options, map: exposes underlying DeleteValue options via a map of keywords.
    see: http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/DeleteValue.Option.html"
  ([client loc options]
   @(delete-async client loc options))
  ([client loc]
   (delete client loc {})))
