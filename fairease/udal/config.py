from pathlib import Path


class Config:

    cache_dir: Path | None
    blue_cloud_token: str | None
    beacon_token: str | None

    def __init__(self):
        self.cache_dir = None
        self.blue_cloud_token = None
        self.beacon_token = None


