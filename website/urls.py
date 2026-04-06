from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('matches/', views.matches, name='matches'),
    path('match/<int:fixture_id>/', views.match_detail, name='match_detail'), 
    path('record/', views.record, name='record'),
    path('accumulators/', views.accumulators, name='accumulators'),
    path('winners/', views.winners, name='winners'),
    path('review/', views.review, name='review'),
    path('donate/', views.donate, name='donate'),
]
