from abc import ABC, abstractmethod

EC_DEPEND_TYPE_DEFINED = "defined"
EC_DEPEND_TYPE_IMPUTED = "imputed"
INCIDENT_REPORT_TYPE_FAIL = "FAIL"
STORE_OPEN_EQUIP_ID = "store_open"

class BaseHandler(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def handle_equip_class(self, type, ecid, depend_ecids=None):
        pass

    @abstractmethod
    def handle_prod_class(self, type, pc_id, depend_ecids=None):
        pass

    @abstractmethod
    def handle_store_class(self, type, sc_id):
        pass

    @abstractmethod
    def handle_store(self, type, s_id, sc_id):
        pass

