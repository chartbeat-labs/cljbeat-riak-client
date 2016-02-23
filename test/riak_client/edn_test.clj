(ns riak-client.edn-test
  (:require [clojure.test :refer :all]
            [riak-client.core :as riak-client]
            [riak-client.edn :refer :all]))


(deftest test-store-edn-async
  (testing "encodes and stores map as edn"
    (let [stored-robj (atom nil)]
      (with-redefs [riak-client/store-async
                    (fn [client loc data options]
                      (future (reset! stored-robj data)))]

        @(store-edn-async nil nil {:a 10 :b ["hello world"]})
        (is (= "application/edn" (.getContentType @stored-robj)))
        (is (= "{:a 10, :b [\"hello world\"]}"
               (#'riak-client/to-clojure @stored-robj)))))))


(deftest test-store-edn
  (testing "encodes and stores map as edn"
    (let [stored-robj (atom nil)]
      (with-redefs [riak-client/store (fn [client loc data options]
                                        (reset! stored-robj data))]

        (store-edn nil nil {:a 10 :b ["hello world"]})
        (is (= "application/edn" (.getContentType @stored-robj)))
        (is (= "{:a 10, :b [\"hello world\"]}"
               (#'riak-client/to-clojure @stored-robj)))))))


(deftest test-fetch-edn-async
  (testing "fetches and decodes edn as a map"
      (with-redefs [riak-client/fetch-async
                    (fn [client loc options]
                      (future ["{:a 10, :b [\"hello world\"]}"]))]

        (is (= [{:a 10 :b ["hello world"]}]
               @(fetch-edn-async nil nil))))))


(deftest test-fetch-edn
  (testing "fetches and decodes edn as a map"
      (with-redefs [riak-client/fetch
                    (fn [client loc options]
                      ["{:a 10, :b [\"hello world\"]}"])]

        (is (= [{:a 10 :b ["hello world"]}]
               (fetch-edn nil nil))))))


(deftest test-ffetch-edn-async
  (testing "fetches and decodes edn as a map"
      (with-redefs [riak-client/ffetch-async
                    (fn [client loc options]
                      (future "{:a 10, :b [\"hello world\"]}"))]

        (is (= {:a 10 :b ["hello world"]}
               @(ffetch-edn-async nil nil))))))


(deftest test-ffetch-edn
  (testing "fetches and decodes edn as a map"
      (with-redefs [riak-client/ffetch
                    (fn [client loc options]
                      "{:a 10, :b [\"hello world\"]}")]

        (is (= {:a 10 :b ["hello world"]}
               (ffetch-edn nil nil))))))
