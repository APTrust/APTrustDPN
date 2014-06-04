from django.contrib import admin
from dpn_workflows.models import SendFileAction, ReceiveFileAction, NodeInfo

class SendFileActionAdmin(admin.ModelAdmin):
    pass
admin.site.register(SendFileAction, SendFileActionAdmin)

class ReceiveFileActionAdmin(admin.ModelAdmin):
    pass
admin.site.register(ReceiveFileAction, ReceiveFileActionAdmin)

class NodeInfoAdmin(admin.ModelAdmin):
    list_display = ['name', 'slug']
admin.site.register(NodeInfo, NodeInfoAdmin)
