class ChannelModel:
    def __init__(self, name, driver, params, description):
        self.name = name
        self.driver_type = driver
        self.params = params or {}
        self.description = description

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data.get("name", ""),
            driver=data.get("driver", ""),
            params=data.get("params", {}),
            description=data.get("description", "")
        )

    def to_dict(self):
        return {
            "name": self.name, "driver": self.driver_type,
            "params": self.params, "description": self.description
        }