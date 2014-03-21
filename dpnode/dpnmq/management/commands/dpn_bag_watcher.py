import os
import sys
import time
import logging
from uuid import uuid4

from django.core.management.base import BaseCommand

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from dpnode.settings import DPN_BAGS_DIR
from dpnode.settings import DPN_BAGS_FILE_EXT

from dpn_workflows.tasks.outbound import initiate_ingest

class Command(BaseCommand):
    help = 'Checks for new bags deposited in a directory'

    def handle(self, *args, **options):
        event_handler = DPNFileEventHandler(patterns=DPN_BAGS_FILE_EXT)
        observer = Observer()
        observer.schedule(event_handler, DPN_BAGS_DIR, recursive=False)
        observer.start()
        print("Watching for new bags...")
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
            # TODO: add filesize validations and catch possible errors
            file_size = os.path.getsize(event.src_path)
            initiate_ingest(uuid4(), file_size)