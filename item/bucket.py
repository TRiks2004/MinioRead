from datetime import datetime
from typing import List
from item.item import str_cls


class BucketContentsOwnerInfo:

    display_name: str
    id: str

    def __init__(self, owner: dict) -> None:
        self.display_name = owner["DisplayName"]
        self.id = owner["ID"]

    def __str__(self) -> str:
        return str_cls(self)

    def __repr__(self) -> str:
        return self.__str__()


class BucketContentsInfo:

    key: str
    last_modified: datetime
    e_tag: str
    size: int
    owner: BucketContentsOwnerInfo
    storage_class: str

    def __init__(self, contents: dict) -> None:
        self.key = contents["Key"]
        self.last_modified = datetime.strptime(
            contents["LastModified"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        self.e_tag = contents["ETag"]
        self.size = contents["Size"]
        self.owner = BucketContentsOwnerInfo(contents["Owner"])
        self.storage_class = contents["StorageClass"]

    def __str__(self) -> str:
        return str_cls(self)

    def __repr__(self) -> str:
        return self.__str__()


class BucketInfo:

    name: str
    prefix: str
    marker: str
    max_keys: int
    is_truncated: bool
    contents: List[BucketContentsInfo] | None

    def __init__(self, xml_dict: dict) -> None:

        __block: dict = xml_dict["ListBucketResult"]

        self.name = __block["Name"]
        self.prefix = __block["Prefix"]
        self.marker = __block["Marker"]
        self.max_keys = int(__block["MaxKeys"])
        self.is_truncated = __block["IsTruncated"]

        contents = __block.get("Contents", None)

        if contents is not None:
            if isinstance(contents, list):
                self.contents = [BucketContentsInfo(c) for c in contents]
            elif isinstance(contents, dict):
                self.contents = [BucketContentsInfo(contents)]
        else:
            self.contents = None

    def __str__(self) -> str:
        return str_cls(self)

    def __repr__(self) -> str:
        return self.__str__()
