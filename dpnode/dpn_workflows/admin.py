from django.contrib import admin
from dpn_workflows.models import SendFileAction, ReceiveFileAction, NodeInfo

class SendFileActionAdmin(admin.ModelAdmin):
    list_display = ('ingest', 'node', 'step', 'state', 'chosen_to_transfer', 'updated_at')
    list_filter = ('step', 'state', 'node', 'chosen_to_transfer')
admin.site.register(SendFileAction, SendFileActionAdmin)

class ReceiveFileActionAdmin(admin.ModelAdmin):
    list_display = ('correlation_id', 'node', 'step', 'state', 'updated_at')
    list_filter = ('step', 'state', 'node')
admin.site.register(ReceiveFileAction, ReceiveFileActionAdmin)

class NodeInfoAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug')
admin.site.register(NodeInfo, NodeInfoAdmin)
