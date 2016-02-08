(ns pipeline
  (:use     [streamparse.specs])
  (:gen-class))

(defn pipeline [options]
   [
    ;; spout configuration
    {"image-spout" (python-spout-spec
          options
          "spouts.new_images.NewImageSpout"
          ["page_url", "image_url"]
          )
    }
    ;; bolt configuration
    {
    "image-bolt" (python-bolt-spec
          options
          {"image-spout" :shuffle}
          "bolts.image.NewImageBolt"
          {"person_args" ["person_args", "image_id", "image_url"]
           "image_obj" ["image_obj", "image_id"]}
          :p 1
          )
    }
    (comment
    "person-bolt" (python-bolt-spec
          options
          {["image-bolt" "person_args"] :shuffle}
          "bolts.person.PersonBolt"
          {"item args" ["item_args", "person_id"]
           "person_obj" ["person_obj", "person_id", "image_id"]}
          :p 3
          )

    "item-bolt" (python-bolt-spec
          options
          {["person-bolt" "item_args"] :shuffle}
          "bolts.item.ItemBolt"
          ["item", "person_id"]
          :p 3
          )

    "merge-items-bolt" (python-bolt-spec
          options
          {"item-bolt" ["person_id"]
           ["person-bolt" "person_obj"] ["person_id"]}
          "bolts.person.MergeItems"
          ["person", "image_id"]
          :p 1
          )

    "merge-people-bolt" (python-bolt-spec
          options
          {"merge-items-bolt" ["image_id"]
           ["image-bolt" "image_obj"] ["image_id"]}
          "bolts.image.MergePeople"
          []
          :p 1
          )
    )
  ]
)
