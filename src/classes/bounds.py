import dataclasses


@dataclasses.dataclass
class Bounds:
    column: str
    min: float
    max: float
