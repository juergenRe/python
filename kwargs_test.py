dd = {"aa": 12, "bb": 45, "cc": "qwertz"}

def f1(p: str, aa: int = 0, cc: str= '00', **kwargs):
    print(f"p:{p} aa:{aa} cc:{cc}")

def f2(p: str, aa: int = 0, cc: str= '00', ee: int=123, **kwargs):
    print(f"p:{p} aa:{aa} cc:{cc}")

f1('r', **dd)
f2('s', ee=987, **dd)
