(ns failedcount
  (:use     [streamparse.specs])
  (:gen-class))

(defn failedcount [options]
   [
    ;; spout configuration
    {"image-spout" (python-spout-spec
          options
          "spouts.failed_images.FailedImageSpout"
          ["image_url", "origin"]
          )
    }
    ;; bolt configuration
    {"origin-count-bolt" (python-bolt-spec
          options
          {"image-spout" ["origin"]}
          "bolts.origincount.OriginCounter"
          ["origin" "count"]
          :p 2
          )
    }
  ]
)
