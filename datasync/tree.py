import yaml
from collections import OrderedDict


class Tree(OrderedDict):
    def __init__(self, privates = None):
        super(Tree, self).__init__()
        self.__privates = [] if privates is None else privates

    @property
    def privates(self): return self.__privates

    def __str__(self): return yaml.dump(self)

    __repr__ = __str__





def _represent_tree(dumper, data):
    privates = data.privates
    value = []
    for item_key, item_value in data.items():
        # check private
        private = False
        for p in privates:
            if not item_key.startswith(p): continue
            private = True
            break
        if private: continue

        # find node
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)
        value.append((node_key, node_value))

    return yaml.nodes.MappingNode(u'tag:yaml.org,2002:map', value)

yaml.add_representer(Tree, _represent_tree)