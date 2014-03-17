from django.contrib import admin
from dpn_workflows.models import IngestAction, NodeInfo

class IngestActionAdmin(admin.ModelAdmin):
    pass

# admin.site.register(IngestAction, IngestActionAdmin)

class NodeInfoAdmin(admin.ModelAdmin):
    list_display = ['name', 'slug']
admin.site.register(NodeInfo, NodeInfoAdmin)
