import os
import time
import logging

from django.core.management.base import BaseCommand
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from dpn_registry.utils import create_entry
from dpnode.settings import (
    DPN_INGEST_DIR_OUT, DPN_BAGS_FILE_EXT,
    DPN_MAX_SIZE, DPN_TTL, DPN_MSG_TTL
)
from dpn_workflows.tasks.outbound import (
    initiate_ingest, choose_and_send_location
)

logger = logging.getLogger('dpnmq.console')


class Command(BaseCommand):
    help = 'Checks for new bags deposited in a directory'

    def handle(self, *args, **options):
        pattern = ['*.%s' % DPN_BAGS_FILE_EXT]
        event_handler = DPNFileEventHandler(patterns=pattern)
        observer = Observer()
        observer.schedule(event_handler, DPN_INGEST_DIR_OUT, recursive=False)
        observer.start()

        print("Watching for new bags (%s). Press CTRL+C to exit." % DPN_INGEST_DIR_OUT)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            print("Good Bye. No more bag watching!")
        observer.join()


class DPNFileEventHandler(PatternMatchingEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            bag_error = False
            filename = base = os.path.basename(event.src_path)
            initial_filesize = os.path.getsize(event.src_path)

            if type(filename) == bytes:
                filename = filename.decode('utf-8')

            logger.info(
                "New bag detected: %s. Let's wait 5 seconds and check size again..."
                % base
            )
            while True:
                # NOTE: how long should we wait to get the final size of the bag??
                # discuss this with the team

                # wait 5 seconds to check bag size again
                time.sleep(5)
                try:
                    filesize_now = os.path.getsize(event.src_path)

                    # if initial filesize is equal to the filesize now
                    # we can start the bag ingestion
                    if initial_filesize == filesize_now:
                        filesize = filesize_now
                        break
                    else:
                        initial_filesize = filesize_now
                        print("Bag is not ready, check again in 5 seconds...")
                except Exception as err:
                    bag_error = err
                    break

            if bag_error:
                logger.error(
                    "Error processing the new bag %s. Msg -> %s" 
                    % (base, bag_error)
                )
            elif filesize < DPN_MAX_SIZE:
                print("Creating registry entry about new detected bag")
                filename_id = os.path.splitext(filename)[0]
                create_entry(filename_id, event.src_path)

                # start ingestion and link task to choose nodes                
                logger.info("Registry entry created. Starting ingestion of %s..." % base)
                delay = DPN_MSG_TTL.get('replication-init-query', DPN_TTL)

                initiate_ingest.apply_async(
                    (filename_id, filesize),
                    link=choose_and_send_location.subtask((), countdown=delay)
                )
                # execute choose_and_send_location task DPN_TTL seconds after 
                # ReplicationInitQuery has been sent to broadcast queue
                # using countdown parameter of celery task to do that

            else:
                logger.info(
                    "Bag %s is too big to be replicated. Not ingested!" % base)
