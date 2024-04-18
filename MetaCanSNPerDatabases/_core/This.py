


from typing import Any, Self, Callable
import itertools

__all__ = ["this"]

def fformat(a : tuple, d : dict, sep : str=", "):
    return sep.join(itertools.chain(map(str, a), map("{0[0]}={0[1]}".format, d.items())))
def fformat(a : tuple, d : dict, sep : str=", "):
    return sep.join(itertools.chain(map(str, a), map("{0[0]}={0[1]}".format, d.items())))

class RecursiveCall:
    callables : list[Callable]
    def __init__(self, callables):
        self.callables = callables
    def __call__(self, obj):
        for callable in self.callables:
            obj = callable(obj)
        return obj

class Attribute:
    name : str
    def __init__(self, name):
        self.name = name
    def __call__(self, obj):
        return getattr(obj, self.name)
    def __str__(self) -> str:
        return f".{self.name}"

class Call:
    args : tuple
    kwargs : dict
    def __init__(self, args, kwargs):
        self.args, self.kwargs = args, kwargs
    def __call__(self, obj):
        return obj(*self.args, **self.kwargs)
    def __str__(self) -> str:
        return f"({fformat(self.args, self.kwargs)})"

class Index:
    key : tuple|slice|Any
    def __init__(self, key):
        self.key = key
    def __call__(self, obj):
        return obj[self.key]
    def __str__(self) -> str:
        if isinstance(self.key, tuple):
            return f"[{', '.join(map(str, self.key))}]"
        elif isinstance(self.key, slice):
            return f"[{self.key.start}:{self.key.stop if self.key.step is None else f'{self.key.stop}:{self.key.step}'}]"
        else:
            return f"[{self.key}]"

oGet = object.__getattribute__

class this: pass
class ThisBase:
    actions : list
    def __new__(cls, *actions) -> None:
        self = super().__new__(cls)
        self.actions = list(actions)
        return self
    def __iter__(self):
        yield RecursiveCall(oGet(self, "actions"))
    def __next__(self):
        return RecursiveCall(oGet(self, "actions"))
    def __repr__(self):
        return f"<{oGet(oGet(self, '__class__'), '__name__')}{''.join(map(str, oGet(self, 'actions')))} at {hex(hash(self))}>"

    def __getattribute__(self, name: str) -> Self:
        oGet(self, "actions").append(Attribute(name))
        return self
    def __call__(self, *args, **kwargs) -> Self:
        oGet(self, "actions").append(Call(args, kwargs))
        return self
    def __getitem__(self, key: Any) -> Self:
        oGet(self, "actions").append(Index(key))
        return self

class ThisType(type):
    """"""
    def __getattribute__(self, name: str) -> this:
        return oGet(ThisBase, "__new__")(self, Attribute(name))
    def __call__(self, *args: Any, **kwds: Any) -> this:
        return oGet(ThisBase, "__new__")(self, Call(args, kwds))
    def __getitem__(self, key: Any) -> this:
        return oGet(ThisBase, "__new__")(self, Index(key))
    def __repr__(self):
        return "<'this' - A Magical Class>"


this = ThisType("this", (ThisBase,), {})

if __name__ == "__main__":
	
	class Test:
		def __init__(self, i):
			self._d = {"*":i}
		def wow(self, x):
			return {key:value*x for key,value in self._d.items()}
	
	print(this)
	print(this.att)
	print(this(1,2,h=8))
	print(this[1,3])
	print(this.att(1,2,h=8)[1,3])

	for item in map(*this.wow(2)["*"], [Test(i) for i in range(10)]):
		print(item)