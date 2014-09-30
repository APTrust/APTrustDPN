from django.db.models import Sum, Max, Avg, Count
from django.shortcuts import render_to_response
from django.contrib.auth.decorators import login_required
from django.template import RequestContext

from dpn_registry.models import RegistryEntry

@login_required
def index(request):
    entries = RegistryEntry.objects.all()
    totals = entries.aggregate(Sum('bag_size'), Max('bag_size'), Avg('bag_size'))

    node_totals = entries.values('first_node_name' ).order_by('first_node_name').annotate(Sum('bag_size'), Max('bag_size'), Avg('bag_size'), Count("bag_size"))
    return render_to_response("index.html", {
        'count': entries.count(),
        'totals': totals,
        'node_totals': node_totals,
        },
        context_instance=RequestContext(request)
    )