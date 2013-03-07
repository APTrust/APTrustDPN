from random import randint

from django.core.management.base import BaseCommand

from dpnmq.messages import QueryForReplication

class Command(BaseCommand):
    help = 'Sends a single broadcast message.'

    def handle(self, *args, **options):
        msg = QueryForReplication()
        msg.request(1024)
        msg.send()
