(ns riak-client
  (:import [clojure.lang Keyword Symbol]
           [com.basho.riak.client.api RiakClient]
           [com.basho.riak.client.api.commands.kv FetchValue$Builder
                                                  StoreValue$Builder
                                                  DeleteValue$Builder]
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


(defn- wrap-future
  "Helper function that wraps a future with a new future which calls f on the
  future's value."
  [fut f]
  (letfn [(deref-future ; ripped from clojure.core
            ([^java.util.concurrent.Future fut]
             (.get fut))
            ([^java.util.concurrent.Future fut timeout-ms timeout-val]
             (try (.get fut timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
                  (catch java.util.concurrent.TimeoutException e
                    timeout-val))))]
    (reify
      clojure.lang.IDeref
      (deref [_] (f (deref-future fut)))

      clojure.lang.IBlockingDeref
      (deref [_ timeout-ms timeout-val]
        (f (deref-future fut timeout-ms timeout-val)))

      clojure.lang.IPending
      (isRealized [_] (.isDone fut))

      java.util.concurrent.Future
      (get [_] (f (.get fut)))
      (get [_ timeout unit] (f (.get fut timeout unit)))
      (isCancelled [_] (.isCancelled fut))
      (isDone [_] (.isDone fut))
      (cancel [_ interrupt?] (.cancel fut interrupt?)))))


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


(def STRING_CONTENT_TYPE "text/plain")

(defn ^RiakObject riak-object
  "Creates a RiakObject with optional content-type."
  ([value]
   (condp instance? value
     ;; if it's a RiakObject already, don't do a thing
     RiakObject value
     ;; if it's a String, set the contentType so it comes back out as a string
     java.lang.String (riak-object value "text/plain")
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
         RiakObject   (if (= (.getContentType obj) STRING_CONTENT_TYPE)
                        ;; if string contentType, just make it a string
                        (.toString (.getValue obj))
                        ;; otherwise, let bytes-to-string decide
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


(defn fetch-async
  "Asynchronous fetch from riak. Returns a future that when derefed will contain
  a sequence of resulting siblings from the request.

  Required:
    - client, RiakClient: see connect
    - loc, vector: see parse-loc-vector

  Optional:
    - options, map: TODO"
  ([client loc options]
   (let [builder (FetchValue$Builder. (parse-loc-vec loc))
         ;; TODO set options here
         cmd (.build builder)
         fut (execute-async client cmd)]
     ;(wrap-future fut get-values)))
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
    - options, map: TODO"
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


(defn store-async
  "Stores binary value or RiakObject at location loc. Returns a future.

  Required:
    - client, RiakClient: see connect
    - loc, vector: see parse-loc-vector
    - store-value, binary OR RiakObject (to make, see: riak-object)

  Optional:
    - options, map: TODO"
  ([client loc store-value options]
   (let [riak-obj (riak-object store-value)
         builder (StoreValue$Builder. riak-obj)
         builder (.withLocation builder (parse-loc-vec loc))
         ;; TODO set options here
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
    - options, map: TODO"
  ([client loc store-value options]
   @(store-async client loc store-value options))
  ([client loc store-value]
   (store client loc store-value {})))


(defn delete-async
  "Deletes value at location loc. Returns a future.

  Required:
  - client, RiakClient: see connect
  - loc, vector: see parse-loc-vector

  Optional:
  - options, map: TODO"
  ([client loc options]
   (let [builder (DeleteValue$Builder. (parse-loc-vec loc))
         ;; TODO set options here
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
  - options, map: TODO"
  ([client loc options]
   @(delete-async client loc options))
  ([client loc]
   (delete client loc {})))
