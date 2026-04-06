from django.contrib.sitemaps import Sitemap
from fixtures.models import Fixture
from django.urls import reverse

class StaticSitemap(Sitemap):
    changefreq = 'daily'
    def items(self):
        return ['home','matches','record','accumulators',
                'winners','review','donate']
    def location(self, item):
        return reverse(item)

class FixtureSitemap(Sitemap):
    changefreq = 'daily'
    priority = 0.7
    def items(self):
        return Fixture.objects.filter(
            status__in=['scheduled','finished']
        ).order_by('-kickoff')[:500]
    def location(self, obj):
        return f'/match/{obj.pk}/'
    def lastmod(self, obj):
        return obj.kickoff

