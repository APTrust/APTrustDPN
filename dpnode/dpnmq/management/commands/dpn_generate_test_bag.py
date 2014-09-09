from uuid import uuid4

from django.core.management.base import BaseCommand

from dpnmq.utils import human_to_bytes
from dpnode.settings import DPN_INGEST_DIR_OUT, DPN_BAGS_FILE_EXT


class Command(BaseCommand):
    help = 'Generate a bag of a specified filesize. Needs a human-readable filesize as argument (e.g. 1G).'

    def handle(self, *args, **options):
        file_size = human_to_bytes(args[0])
        bag_name = "%s/%s.%s" % (DPN_INGEST_DIR_OUT, uuid4(), DPN_BAGS_FILE_EXT)
        f = open(bag_name, "wb")
        f.seek(file_size - 1)
        f.write("\0".encode('utf-8'))
        f.close()