
from abc import ABC

class Connection(ABC):

    def __init__(self) -> None:
        super().__init__()


    def to_string(self):
        pass

    def __print__(self):
        pass