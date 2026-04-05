from django.urls import path
from . import views

urlpatterns = [
    path('',                        views.home,         name='home'),
    path('matches/',                views.matches,      name='matches'),
    path('match/<int:fixture_id>/', views.match_detail, name='match_detail'),
    path('record/',                 views.record,       name='record'),
    path('accumulators/',           views.accumulators, name='accumulators'),
    path('donate/',                 views.donate,       name='donate'),
    path('winners/',                views.winners,      name='winners'), 
]
