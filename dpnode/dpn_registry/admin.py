from django.contrib import admin
from dpn_registry.models import Node, RegistryEntry, NodeEntry

class NodeAdmin(admin.ModelAdmin):
    pass
admin.site.register(Node, NodeAdmin)

class RegistryEntryAdmin(admin.ModelAdmin):
    list_display = ('dpn_object_id', 'first_node_name', 'bag_size', 'last_modified_date')
    list_filter = ('first_node_name',)
admin.site.register(RegistryEntry, RegistryEntryAdmin)

class NodeEntryAdmin(admin.ModelAdmin):
    list_display = ('dpn_object_id', 'first_node_name', 'node', 'bag_size', 'last_modified_date')
    list_filter = ('first_node_name', 'node')
admin.site.register(NodeEntry, NodeEntryAdmin)
