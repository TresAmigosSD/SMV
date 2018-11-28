from smv.provider import SmvProvider

class SomeProvider(SmvProvider):
    @staticmethod
    def provider_type(): return "some"
