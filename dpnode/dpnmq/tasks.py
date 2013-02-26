class TaskRouter:
    def __init__(self):
        self._registry = {}

    def register(self, key, klass, **options):
        self._registry[key] = klass
        pass

    def unregister(self, key):
        del self._registry[key]

    def dispatch(self, key, msg):
        handler = self._registry.get(key, default_handler)
        if handler:
            handler(msg)


router = TaskRouter()


def default_handler(msg):
    print("Default Message.")


router.register('default', default_handler)


def info_handler(msg):
    print("DELIVERY INFO: %r" % msg.delivery_info)
    print("DELIVERY TAG: %r" % msg.delivery_tag)
    print("CONTENT TYPE: %r" % msg.content_type)
    print("HEADERS INFO: %r" % msg.headers)
    print("PROPERTIES INFO %r" % msg.properties)
    print("BODY INFO: %r" % msg.payload)
    print("PAYLOAD: %r" % msg.payload)
    print("-------------------------------")


router.register('info', info_handler)

# Temporary
router.register('is_available_replication', info_handler)