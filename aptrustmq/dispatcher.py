# aptrustmq/dispatcher.py
# Moudule for dispatching tasks to relevant task handlers.

# For now just going with a dict that I'll use a get on for dispatching.
MSG_REGISTRY = {
	'default': default_handler(),
	'is_available_replication': None,
	'content_location': None,
	'request_for_registry_update': None,
}

def default_handler():
	return "Message not recognized.  Using default handler."