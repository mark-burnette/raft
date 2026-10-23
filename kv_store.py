class KeyValueStore:
    def __init__(self):
        # no lock of its own: the owning Server only touches it while holding Server.lock
        self.store = {}

    @staticmethod
    def parse(request):
        try:
            op = request['op']
            key = request['key']

            if op in ('get', 'delete'):
                return {'op': op, 'key': key}
            elif op == 'set':
                return {'op': op, 'key': key, 'val': request['val']}
        except Exception as e:
            print("KeyValueStore.parse()", e)

    @staticmethod
    def is_read(cmd):
        return cmd['op'] == 'get'

    def apply(self, cmd):
        try:
            if cmd['op'] == 'get':
                return self.get(cmd['key'])
            elif cmd['op'] == 'set':
                self.set(cmd['key'], cmd['val'])
                return f"{cmd['key']} set to {cmd['val']}"
            elif cmd['op'] == 'delete':
                self.delete(cmd['key'])
                return f"{cmd['key']} deleted"

            return "unknown command"
        except Exception as e:
            print("KeyValueStore.apply()", e)

    # a JSON-serializable copy of the whole store, for a snapshot
    def snapshot(self):
        return dict(self.store)

    # replace the whole store with a snapshot's
    def restore(self, data):
        self.store = dict(data)

    def get(self, key):
        return self.store.get(key, None)

    def set(self, key, val):
        self.store[key] = val

    def delete(self, key):
        self.store.pop(key, None)
