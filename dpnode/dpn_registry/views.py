from django.db.models import Sum, Max, Avg, Count
from django.shortcuts import render_to_response
from django.contrib.auth.decorators import login_required
from django.template import RequestContext

from dpn_registry.models import RegistryEntry, Node


@login_required
def index(request):
    entries = RegistryEntry.objects.all()
    totals = entries.aggregate(Sum('bag_size'), Max('bag_size'),
                               Avg('bag_size'))

    node_totals = entries.values('first_node_name').annotate(Sum('bag_size'),
                                                             Max('bag_size'),
                                                             Avg('bag_size'),
                                                             Count("bag_size"))

    local_totals = Node.objects.annotate(
        bag_count=Count('registryentry__bag_size'),
        total_size=Sum('registryentry__bag_size'))


    return render_to_response("index.html", {
        'count': entries.count(),
        'totals': totals,
        'local_totals': local_totals,
        'node_totals': node_totals,
    },
                              context_instance=RequestContext(request)
    )