from django.contrib import admin
from dpn_registry.models import Node, RegistryEntry, NodeEntry

class NodeAdmin(admin.ModelAdmin):
    pass
admin.site.register(Node, NodeAdmin)

class RegistryEntryAdmin(admin.ModelAdmin):
    pass
admin.site.register(RegistryEntry, RegistryEntryAdmin)

class NodeEntryAdmin(admin.ModelAdmin):
    pass
admin.site.register(NodeEntry, NodeEntryAdmin)
