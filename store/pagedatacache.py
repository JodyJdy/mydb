import threading


class PageDataCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._dict = {}
    def set(self, container_id,page_id, value):
        with self._lock:
            self._dict[(container_id,page_id)] = value
    def get(self,  container_id,page_id, default=None):
        with self._lock:
            return self._dict.get((container_id,page_id), default)
    def delete(self,  container_id,page_id):
        with self._lock:
            if (container_id,page_id) in self._dict:
                del self._dict[(container_id,page_id)]
    def items(self):
        with self._lock:
            return list(self._dict.items())
    def keys(self):
        with self._lock:
            return list(self._dict.keys())
    def values(self):
        with self._lock:
            return list(self._dict.values())


page_data_cache = PageDataCache()

