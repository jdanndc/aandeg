from handler.base import BaseHandler


class CollectHandler(BaseHandler):
    def __init__(self):
        self.equip_class_collect_list = []
        self.prod_class_collect_list = []
        self.store_class_collect_list = []
        self.store_collect_list = []

    def handle_equip_class(self, type, ecid, depend_ecids=None):
        d = {'type':type, 'ecid':ecid, 'depend_ecids':depend_ecids}
        self.equip_class_collect_list.append(d)

    def handle_prod_class(self, type, pc_id, depend_ecids=None):
        d = {'type':type, 'pc_id':pc_id, 'depend_ecids':depend_ecids}
        self.prod_class_collect_list.append(d)

    def handle_store_class(self, type, sc_id):
        d = {'type':type, 'sc_id':sc_id}
        self.store_class_collect_list.append(d)

    def handle_store(self, type, s_id, sc_id):
        d = {'type':type, 's_id':s_id, 'sc_id':sc_id}
        self.store_collect_list.append(d)