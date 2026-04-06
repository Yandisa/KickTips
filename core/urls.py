from django.contrib import admin
from django.urls import path, include
from django.contrib.sitemaps.views import sitemap
from website.sitemaps import StaticSitemap, FixtureSitemap

sitemaps = {
    'static': StaticSitemap,
    'fixtures': FixtureSitemap,
}

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('website.urls')),
    path('sitemap.xml', sitemap, {'sitemaps': sitemaps},
         name='django.contrib.sitemaps.views.sitemap'),
]
