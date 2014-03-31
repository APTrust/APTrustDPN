import os
import sys
import time
import logging

from uuid import uuid4

from django.core.management.base import BaseCommand

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from dpnode.settings import DPN_BAGS_DIR, DPN_BAGS_FILE_EXT
from dpnode.settings import DPN_MAX_SIZE

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
            initial_filesize = os.path.getsize(event.src_path)            
            filename = os.path.splitext(base)[0] # filename to be used as id

            logger.info("New bag detected: %s. Let's wait 5 seconds and check size again..." % base)
            while True:
                # wait 5 seconds to check bag size again
                time.sleep(5)
                filesize_now = os.path.getsize(event.src_path)
                
                # if initial filesize is equal to the filesize now
                # we can start the bag ingestion
                if initial_filesize == filesize_now:
                    filesize = filesize_now
                    break
                else:
                    initial_filesize = filesize_now
                    print("Bag is not ready, check again in 5 seconds...")

                # NOTE: how long should we wait to get the final size of the bag??
                # discuss this with the team
            
            if filesize < DPN_MAX_SIZE:
                initiate_ingest(filename, filesize)
                logger.info("Bag size looks good... starting ingestion of %s." % base)
            else:
                logger.info("Bag %s is too big to be replicated. Not ingested!" % base)