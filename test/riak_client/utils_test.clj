(ns riak-client.utils-test
  (:require [clojure.test :refer :all]
            [riak-client.utils :refer :all]))

(deftest test-wrap-future
  (testing "wraps the future with another function"
    (is "foobarbaz" @(wrap-future (future "barbaz") (partial str "foo")))))
