class TagModel:
    def __init__(self, name, address, data_type, scan_rate, description, scaling=None, access="Read/Write"):
        self.name = name
        self.address = address
        self.data_type = data_type
        self.scan_rate = scan_rate
        self.description = description
        self.scaling = scaling or {"type": "None"}
        self.access = access or "Read/Write"

    @classmethod
    def from_dict(cls, data):
        gen = data.get("general", {})
        return cls(
            name=gen.get("name", ""),
            address=gen.get("address", ""),
            data_type=gen.get("data_type", "Word"),
            scan_rate=gen.get("scan_rate", "10"),
            description=gen.get("description", ""),
            scaling=data.get("scaling"),
            access=gen.get("access", data.get("access", "Read/Write")),
        )

    def to_dict(self):
        return {
            "general": {
                "name": self.name,
                "address": self.address,
                "data_type": self.data_type,
                "scan_rate": self.scan_rate,
                "description": self.description,
                "access": self.access,
            },
            "scaling": self.scaling,
        }