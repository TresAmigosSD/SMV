from smv.provider import SmvProvider

class MyBaseProvider(SmvProvider):
    @staticmethod
    def provider_type(): return "aaa"

class MyConcreteProvider(MyBaseProvider):
    @staticmethod
    def provider_type(): return "bbb"
