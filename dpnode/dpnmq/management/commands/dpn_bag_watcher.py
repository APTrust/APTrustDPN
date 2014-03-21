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

logger = logging.getLogger('dpnmq.console')

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
            base = os.path.basename(event.src_path)            
            filesize = os.path.getsize(event.src_path)
            filename = os.path.splitext(base)[0] # filename to be used as id

            # start the ingestion
            initiate_ingest(filename, filesize)

            logger.info("New bag '%s' detected. Starting ingestion..." % base)