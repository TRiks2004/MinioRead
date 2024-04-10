def str_cls(self: type) -> str:
    return (
        f"{self.__class__.__name__}("
        + ", ".join([f"{k}={str(v)}" for k, v in self.__dict__.items()])
        + ")"
    )
