class DeviceModel:
    def __init__(
        self,
        name,
        device_id,
        description,
        timing=None,
        data_access=None,
        encoding=None,
        block_sizes=None,
        ethernet=None,
    ):
        self.name = name
        self.device_id = device_id
        self.description = description
        self.timing = timing or {}
        self.data_access = data_access or {}
        self.encoding = encoding or {}
        self.block_sizes = block_sizes or {}
        self.ethernet = ethernet or {}

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data.get("name", ""),
            device_id=data.get("device_id", 1),
            description=data.get("description", ""),
            timing=data.get("timing"),
            data_access=data.get("data_access"),
            encoding=data.get("encoding"),
            block_sizes=data.get("block_sizes"),
            ethernet=data.get("ethernet")
        )

    def to_dict(self):
        return {
            "name": self.name, "device_id": self.device_id, "description": self.description,
            "timing": self.timing, "data_access": self.data_access, "encoding": self.encoding,
            "block_sizes": self.block_sizes, "ethernet": self.ethernet
        }