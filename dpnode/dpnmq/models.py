"""
    Yeah, well, that's just, like, your opinion, man.
    
            - The Dude

"""

# ------------------------------
# register some signals handlers
# ------------------------------
from celery.signals import after_task_publish

from celery import current_app as celery


@after_task_publish.connect
def update_sent_state(sender=None, body=None, **kwargs):
    """
    Updates task state in order to know if task exists 
    when try to pull the state with AsyncResult
    """

    task = celery.tasks.get(sender)
    backend = task.backend if task else celery.backend
    backend.store_result(body['id'], None, "SENT")