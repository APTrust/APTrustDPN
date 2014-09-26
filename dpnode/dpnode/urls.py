from django.conf.urls import patterns, url, include
from django.conf.urls.static import static
from django.conf import settings

# Uncomment the next two lines to enable the admin:
from django.contrib import admin

admin.autodiscover()

urlpatterns = patterns('',
                       # Examples:
                       # url(r'^$', 'dpnode.views.home', name='home'),
                       # url(r'^dpnode/', include('dpnode.foo.urls')),

                       # Uncomment the admin/doc line below to enable admin documentation:
                       url(r'^admin/doc/',
                           include('django.contrib.admindocs.urls')),

                       # Uncomment the next line to enable the admin:
                       url(r'^admin/', include(admin.site.urls)),

                       # Grappelli URLs
                       url(r'^grappelli/', include('grappelli.urls')),

                       # Login URL
                       url(r'^accounts/login/', 'django.contrib.auth.views.login', {'template_name': 'login.html'}, name="login"),

                       # Registry URLs
                       url(r'^registry/', include('dpn_registry.urls', namespace="registry")),
)

if settings.DEBUG:
    urlpatterns += static('outbound/',
                          document_root=settings.DPN_INGEST_DIR_OUT)