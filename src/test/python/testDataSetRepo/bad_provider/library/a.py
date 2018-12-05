from smv.provider import SmvProvider


class MyBaseProvider(SmvProvider):
    @staticmethod
    def provider_type(): return "aaa"

# ERROR: two classes below with same provider fqn "aaa.bbb"

class MyConcreteProvider1(MyBaseProvider):
    @staticmethod
    def provider_type(): return "bbb"

class MyConcreteProvider2(MyBaseProvider):
    @staticmethod
    def provider_type(): return "bbb"
