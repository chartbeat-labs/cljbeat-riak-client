(ns riak-client-test
  (:require [clojure.test :refer :all]
            [riak-client :refer :all]
            [clojure.reflect :refer :all])
  (:import [com.basho.riak.client.api.commands.kv FetchValue
                                                  FetchValue$Builder
                                                  FetchValue$Option
                                                  StoreValue
                                                  StoreValue$Builder
                                                  StoreValue$Option
                                                  DeleteValue
                                                  DeleteValue$Builder
                                                  DeleteValue$Option]
           [com.basho.riak.client.core.query.crdt.types RiakCounter
                                                        RiakFlag
                                                        RiakSet
                                                        RiakRegister
                                                        RiakMap
                                                        RiakMap$MapEntry]
           [com.basho.riak.client.core.query RiakObject]
           [com.basho.riak.client.core.util BinaryValue]
           [com.basho.riak.client.core.query Namespace Location]))


(defn b-to-s [bs]
  (apply str (map char bs)))

(defn bytes= [bs1 bs2]
  (= (map int bs1) (map int bs2)))


(deftest test-wrap-future
  (testing "wraps the future with another function"
    (is "foobarbaz"
        @(#'riak-client/wrap-future (future "barbaz")
                                    (partial str "foo")))))


(deftest test-binary-value
  (testing "creates BinaryValue correctly for"

    (testing "strings"
      (is (= (BinaryValue/create "foo")
             (#'riak-client/binary-value "foo"))))

    (testing "keywords"
      (is (= (BinaryValue/create "foo")
             (#'riak-client/binary-value :foo))))

    (testing "symbols"
      (is (= (BinaryValue/create "foo")
             (#'riak-client/binary-value 'foo))))

    (testing "byte-arrays"
      (is (= (BinaryValue/create "foo")
             (#'riak-client/binary-value (.getBytes "foo")))))

    (testing "ints"
      (is (= (BinaryValue/create (.. (java.nio.ByteBuffer/allocate 4)
                                     (putInt (int 42))
                                     (array)))
             (#'riak-client/binary-value (int 42)))))))


(deftest test-riak-object
  (testing "creates a RiakObject"
    (is (= (.getValue (.. (RiakObject.) (setValue (BinaryValue/create "foo"))))
           (.getValue (riak-object "foo"))))))


(deftest test-to-clojure
  (testing "when bytes-to-string is false"
    (testing "converts BinaryValue to byte-array"
      (is (bytes= (.getBytes "foo")
                  (#'riak-client/to-clojure (BinaryValue/create "foo")))))

    (testing "converts RiakRegister to byte-array"
      (is (bytes= (.getBytes "foo")
                  (#'riak-client/to-clojure
                    (RiakRegister. (BinaryValue/create "foo"))))))

    (testing "converts RiakCounter to long"
      (is (= 42 (#'riak-client/to-clojure (RiakCounter. 42)))))

    (testing "converts RiakFlag to boolean"
      (is (= false (#'riak-client/to-clojure (RiakFlag. false))))))

  (testing "when bytes-to-string is true"
    (testing "converts BinaryValue to string"
      (is (= "foo"
             (#'riak-client/to-clojure
               (.. (RiakObject.)
                   (setValue (BinaryValue/create "foo")))
               :bytes-to-string))))

    (testing "converts RiakRegister to string"
      (is (= "foo"
             (#'riak-client/to-clojure
               (RiakRegister. (BinaryValue/create "foo"))
               :bytes-to-string))))

    (testing "converts RiakSet to set"
      (is (= #{"foo" "bar" "baz"}
             (#'riak-client/to-clojure (RiakSet. [(BinaryValue/create "foo")
                                                  (BinaryValue/create "bar")
                                                  (BinaryValue/create "baz")])
                                       :bytes-to-string))))

    (testing "converts RiakMap to map"
      (is (= {"a" 1 "b" false "c" {"d" "e"}}
             (#'riak-client/to-clojure
               (RiakMap.
                 [(RiakMap$MapEntry. (BinaryValue/create "a") (RiakCounter. 1))
                  (RiakMap$MapEntry. (BinaryValue/create "b") (RiakFlag. false))
                  (RiakMap$MapEntry.
                    (BinaryValue/create "c")
                    (RiakMap. [(RiakMap$MapEntry.
                                 (BinaryValue/create "d")
                                 (RiakRegister. (BinaryValue/create "e")))]))])
               :bytes-to-string))))))


(deftest test-parse-loc-vec
  (testing "creates location for"
    (testing "bucket and key"
      (is (= (Location. (Namespace. "bar") (BinaryValue/create "baz"))
             (#'riak-client/parse-loc-vec ["bar" "baz"]))))

    (testing "bucket-type, bucket, and key"
      (is (= (Location. (Namespace. "foo" "bar") (BinaryValue/create "baz"))
             (#'riak-client/parse-loc-vec ["foo" "bar" "baz"]))))))


(deftest test-fetch-async
  (let [executed-cmd (atom nil)]
    (with-redefs [riak-client/execute-async
                  (fn [client cmd]
                    (reset! executed-cmd cmd)
                    (future :mock-resp))
                  riak-client/get-values
                  (fn [response]
                    [(.. (RiakObject.)
                         (setValue (BinaryValue/create "hello world")))])]
      (let [fut (fetch-async :mock-client ["foo" "bar" "baz"])
            res @fut
            cmd @executed-cmd]

        (testing "executed correct fetch command"
          (is (= (.build (FetchValue$Builder.
                           (Location. (Namespace. "foo" "bar")
                                      (BinaryValue/create "baz"))))
                 cmd)))

        (testing "returned the correct value"
          ;; it's easier to compare strings than bytes
          (is (= ["hello world"] (map b-to-s res))))))))


(deftest test-ffetch-async
  (with-redefs [riak-client/fetch-async
                (fn [client cmd options]
                  (future ["hello world" "goodbye"]))]

    (testing "returns the first sibling returned by fetch-async"
      (is (= "hello world" @(ffetch-async :mock-client :mock-key))))))


(deftest test-fetch
  (let [executed-cmd (atom nil)]
    (with-redefs [riak-client/execute-async
                  (fn [client cmd]
                    (reset! executed-cmd cmd)
                    (future :mock-resp))
                  riak-client/get-values
                  (fn [response]
                    [(.. (RiakObject.)
                         (setValue (BinaryValue/create "hello world")))])]
      (let [res (fetch :mock-client ["foo" "bar" "baz"])
            cmd @executed-cmd]

        (testing "executed correct fetch command"
          (is (= (.build (FetchValue$Builder.
                           (Location. (Namespace. "foo" "bar")
                                      (BinaryValue/create "baz"))))
                 cmd)))

        (testing "returned the correct value"
          ;; it's easier to compare strings than bytes
          (is (= ["hello world"] (map b-to-s res))))))))


(deftest test-ffetch
  (with-redefs [riak-client/fetch
                (fn [client cmd options]
                  ["hello world" "goodbye"])]

    (testing "returns the first sibling returned by fetch-async"
      (is (= "hello world" (ffetch :mock-client :mock-key))))))


(deftest test-store-async
  (let [executed-cmd (atom nil)]
    (with-redefs [riak-client/execute-async
                  (fn [client cmd]
                    (reset! executed-cmd cmd)
                    (future :mock-resp))]
      (let [store-val (.. (RiakObject.)
                          (setValue (BinaryValue/create "hello world")))
            fut (store-async :mock-client ["foo" "bar" "baz"] store-val)
            _ @fut
            cmd @executed-cmd]

        (testing "executed correct store command"
          (is (= (.. (StoreValue$Builder. store-val)
                     (withLocation (Location. (Namespace. "foo" "bar")
                                              (BinaryValue/create "baz")))
                     (build))
                 cmd)))))))


(deftest test-store
  (let [executed-cmd (atom nil)]
    (with-redefs [riak-client/execute-async
                  (fn [client cmd]
                    (reset! executed-cmd cmd)
                    (future :mock-resp))]
      (let [store-val (.. (RiakObject.)
                          (setValue (BinaryValue/create "hello world")))
            _ (store :mock-client ["foo" "bar" "baz"] store-val)
            cmd @executed-cmd]

        (testing "executed correct store command"
          (is (= (.. (StoreValue$Builder. store-val)
                     (withLocation (Location. (Namespace. "foo" "bar")
                                              (BinaryValue/create "baz")))
                     (build))
                 cmd)))))))


(deftest test-delete-async
  (let [executed-cmd (atom nil)]
    (with-redefs [riak-client/execute-async
                  (fn [client cmd]
                    (reset! executed-cmd cmd)
                    (future :mock-resp))]
      (let [fut (delete-async :mock-client ["foo" "bar" "baz"])
            _ @fut
            cmd @executed-cmd]

        (testing "executed correct delete command"
          (is (= (.build (DeleteValue$Builder.
                           (Location. (Namespace. "foo" "bar")
                                      (BinaryValue/create "baz"))))
                 cmd)))))))


(deftest test-delete
  (let [executed-cmd (atom nil)]
    (with-redefs [riak-client/execute-async
                  (fn [client cmd]
                    (reset! executed-cmd cmd)
                    (future :mock-resp))]
      (let [_ (delete :mock-client ["foo" "bar" "baz"])
            cmd @executed-cmd]

        (testing "executed correct delete command"
          (is (= (.build (DeleteValue$Builder.
                           (Location. (Namespace. "foo" "bar")
                                      (BinaryValue/create "baz"))))
                 cmd)))))))

