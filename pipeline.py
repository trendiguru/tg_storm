"""
Pipeline topology
"""

from streamparse import Grouping, Topology
from bolts.image import ImageBolt, MergePeople
from bolts.person import PersonBolt, MergeItems
from bolts.item import ItemBolt
from spouts.new_images import ImageSpout


# top topology class
class pipeline(Topology):

    # Spouts
    image_spout = ImageSpout.spec()

    # Bolts
    image_bolt = ImageBolt.spec(inputs=[image_spout], par=1)

    person_bolt = PersonBolt(inputs=[image_bolt], par=15)

    item_bolt = ItemBolt(inputs=[person_bolt], par=15)

    merge_items_bolt = MergeItems.spec(inputs=[{person_bolt['person_obj']: Grouping.fields('person_id')},
                                               {item_bolt: Grouping.fields('person_id')}],
                                       par=2)
    merge_people_bolt = MergePeople.spec(inputs=[{image_bolt['image_obj']: Grouping.fields('image_id')},
                                                 {merge_items_bolt: Grouping.fields('image_id')}],
                                       par=2)