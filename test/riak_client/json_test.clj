(ns riak-client.json-test
  (:require [clojure.test :refer :all]
            [riak-client.core :as riak-client]
            [riak-client.json :refer :all]))


(deftest test-store-json-async
  (testing "encodes and stores map as json"
    (let [stored-robj (atom nil)]
      (with-redefs [riak-client/store-async
                    (fn [client loc data options]
                      (future (reset! stored-robj data)))]

        @(store-json-async nil nil {:a 10 :b ["hello world"]})
        (is (= "application/json" (.getContentType @stored-robj)))
        (is (= "{\"a\":10,\"b\":[\"hello world\"]}"
               (#'riak-client/to-clojure @stored-robj)))))))


(deftest test-store-json
  (testing "encodes and stores map as json"
    (let [stored-robj (atom nil)]
      (with-redefs [riak-client/store (fn [client loc data options]
                                        (reset! stored-robj data))]

        (store-json nil nil {:a 10 :b ["hello world"]})
        (is (= "application/json" (.getContentType @stored-robj)))
        (is (= "{\"a\":10,\"b\":[\"hello world\"]}"
               (#'riak-client/to-clojure @stored-robj)))))))


(deftest test-fetch-json-async
  (testing "fetches and decodes json as a map"
      (with-redefs [riak-client/fetch-async
                    (fn [client loc options]
                      (future ["{\"a\":10,\"b\":[\"hello world\"]}"]))]

        (is (= [{"a" 10 "b" ["hello world"]}]
               @(fetch-json-async nil nil))))))


(deftest test-fetch-json
  (testing "fetches and decodes json as a map"
      (with-redefs [riak-client/fetch
                    (fn [client loc options]
                      ["{\"a\":10,\"b\":[\"hello world\"]}"])]

        (is (= [{"a" 10 "b" ["hello world"]}]
               (fetch-json nil nil))))))


(deftest test-ffetch-json-async
  (testing "fetches and decodes json as a map"
      (with-redefs [riak-client/ffetch-async
                    (fn [client loc options]
                      (future "{\"a\":10,\"b\":[\"hello world\"]}"))]

        (is (= {"a" 10 "b" ["hello world"]}
               @(ffetch-json-async nil nil))))))


(deftest test-ffetch-json
  (testing "fetches and decodes json as a map"
      (with-redefs [riak-client/ffetch
                    (fn [client loc options]
                      "{\"a\":10,\"b\":[\"hello world\"]}")]

        (is (= {"a" 10 "b" ["hello world"]}
               (ffetch-json nil nil))))))
