from smv.provider import SmvProvider
from smv import SmvModule

class SomeSemi(SmvProvider):
    @staticmethod
    def provider_type(): return "somesemi"

class ModWithProvider(SmvModule, SmvProvider):
    @staticmethod
    def provider_type(): return "modprov"
