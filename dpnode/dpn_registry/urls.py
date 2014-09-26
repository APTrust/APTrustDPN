from django.conf.urls import include, patterns, url

urlpatterns = patterns('dpn_registry.views',
    url(r'^$', 'index', name="index"),
)
