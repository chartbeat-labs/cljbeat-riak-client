(ns riak-client.utils)

  (defn wrap-future
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
