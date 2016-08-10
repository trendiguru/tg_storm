(ns pipeline
  (:use     [streamparse.specs])
  (:gen-class))

(defn pipeline [options]
   [
    ;; spout configuration
    {
    "image-spout" (python-spout-spec
          options
          "spouts.new_images.NewImageSpout"
          ["page_url", "image_url", "products_collection", "method"]
          )
    }
    ;;"object-image-spout" (python-spout-spec
    ;;      options
    ;;      "spouts.new_objects.NewObjectsImageSpout"
    ;;      ["page_url", "image_url"]
    ;;      )
    ;;}
    ;; bolt configuration
    {
    "image-bolt" (python-bolt-spec
          options
          {"image-spout" :shuffle}
          "bolts.image.NewImageBolt"
          {"person_args" ["person"]
           "image_obj" ["image_obj", "image_id"]}
          :p 1
          )

    ;;"object-image-bolt" (python-bolt-spec
    ;;    options
    ;;    {"object-image-spout" :shuffle}
    ;;     "bolts.object_image.NewObjectsImageBolt"
    ;;    {"to_item_bolt" ["object" "image_id"]
    ;;     "to_merge_objects" ["image_obj", "image_id"]}
    ;;    :p 1
    ;;    )

    "person-bolt" (python-bolt-spec
          options
          {["image-bolt" "person_args"] :shuffle}
          "bolts.person.PersonBolt"
          {"item_args" ["item" "person_id", "image_id"]
           "person_obj" ["person_obj", "person_id", "image_id"]}
	      :p 15
          )

    "item-bolt" (python-bolt-spec
          options
          {["person-bolt" "item_args"] :shuffle}
          "bolts.item.ItemBolt"
          ["item", "person_id"]
          :p 15
          )

    ;; future item bolt:
    ;;"item-bolt" (python-bolt-spec
    ;;      options
    ;;      {["person-bolt" "item_args"] :shuffle
    ;;       ["object-image-bolt" "to_item_bolt"] :shuffle}
    ;;      "bolts.item.ItemBolt"
    ;;      {"to_merge_items" ["item", "person_id"]
    ;;       "to_merge_objects" ["item", "image_id"]}
    ;;      :p 5
    ;;      )

    ;;"merge-objects-bolt" (python-bolt-spec
    ;;      options
    ;;      {["object-image-bolt" "object_obj"] ["image_id"]
    ;;       ["item-bolt" "to_merge_objects"] ["image_id"]}
    ;;      "bolts.object.ObjectBolt"
    ;;      []
    ;;      :p 2
    ;;      )

    "merge-items-bolt" (python-bolt-spec
          options
          {["person-bolt" "person_obj"] ["person_id"]
            "item-bolt" ["person_id"]}
          "bolts.person.MergeItems"
          ["person", "image_id"]
          :p 2
          )

    "merge-people-bolt" (python-bolt-spec
          options
          {["image-bolt" "image_obj"] ["image_id"]
            "merge-items-bolt" ["image_id"]}
          "bolts.image.MergePeople"
          []
          :p 2
          )
    }
  ]
)
