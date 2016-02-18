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
          {"person_args" ["person"]
           "image_obj" ["image_obj", "image_id"]}
          :p 1
          )

    "person-bolt" (python-bolt-spec
          options
          {["image-bolt" "person_args"] :shuffle}
          "bolts.person.PersonBolt"
          {"item_args" ["item" "person_id"]
           "person_obj" ["person_obj", "person_id", "image_id"]}
	      :conf {"topology.message.timeout.secs", 10000}
	      :p 10
          )

    "item-bolt" (python-bolt-spec
          options
          {["person-bolt" "item_args"] :shuffle}
          "bolts.item.ItemBolt"
          ["item", "person_id"]
          :p 10
          )

    "merge-items-bolt" (python-bolt-spec
          options
          {["person-bolt" "person_obj"] ["person_id"]
            "item-bolt" ["person_id"]}
          "bolts.person.MergeItems"
          ["person", "image_id"]
          :p 5
          )

    "merge-people-bolt" (python-bolt-spec
          options
          {["image-bolt" "image_obj"] ["image_id"]
            "merge-items-bolt" ["image_id"]}
          "bolts.image.MergePeople"
          []
          :p 5
          )
    }
  ]
)

;; options =  --option 'topology.workers=2'
;;            --option 'topology.acker.executors=2'
;;            --option 'streamparse.log.path="/home/developer/python-pacakges/tg_storm/logs"'
;;            --option 'streamparse.log.level="debug"'
;;            --option topology.message.timeout.secs=3000
