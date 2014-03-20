import sys
import time
import logging

from django.core.management.base import BaseCommand

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from dpnode.settings import DPN_BAGS_DIR
from dpnode.settings import DPN_BAGS_FILE_EXT

class Command(BaseCommand):
    help = 'Sends a single broadcast message.'

    def handle(self, *args, **options):
        event_handler = DPNFileEventHandler(patterns=DPN_BAGS_FILE_EXT)
        observer = Observer()
        observer.schedule(event_handler, DPN_BAGS_DIR, recursive=False)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            print("Good Bye. No more bags watching!")
        observer.join()

class DPNFileEventHandler(PatternMatchingEventHandler):

    def on_created(self, event):
        if not event.is_directory:
            # create celery task
            pass