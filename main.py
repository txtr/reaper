import datetime
from b2sdk.v2 import InMemoryAccountInfo, B2Api

class Union:
    def __init__(self, keys):
        print(f"Initializing {len(keys)} Accounts")
        self.buckets = [Bucket(mid, mk) for mid, mk in keys]
        self.files = {}

    def cache(self):
        for bucket in self.buckets:
            file_data_map = bucket.get_files()
            for fv_id, info in file_data_map.items():
                name = info['name']
                if name not in self.files:
                    self.files[name] = []
                self.files[name].append(info)

        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"./cache_{timestamp}.txt"

        with open(filename, "w") as f:
            print(self.files, file=f)
        print(f"Cache written to {filename}")


class Bucket:
    def __init__(self, master_key_id: str, master_key: str):
        self.id = master_key_id
        self.info = InMemoryAccountInfo()
        self.api = B2Api(self.info)
        self.api.authorize_account("production", master_key_id, master_key)

        buckets = list(self.api.list_buckets())
        self.bucket = buckets[0]

    def get_files(self):
        files = {}
        for file_version, _ in self.bucket.ls(latest_only=False, recursive=True):
            fv_id = file_version.id_
            files[fv_id] = {
                "name": file_version.file_name,
                "size": file_version.size,
                "uploaded_on": file_version.upload_timestamp,
                "sha1": file_version.content_sha1
            }
        return files


if __name__ == '__main__':
    from KEYS import KEYS

    union = Union(KEYS)
    union.cache()
