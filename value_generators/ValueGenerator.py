from typing import Generator

class ValueGenerator():

    def __init__(self, **kwargs) -> None:
        self.generator = self.create_generator(**kwargs)

    def create_generator(self, **kwargs) -> Generator:
        raise NotImplementedError("Implement me!")
