from django.urls import path
from django.contrib.auth import views as auth_views
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
    path('admin/grade/', views.admin_grade, name='admin_grade'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('login/', auth_views.LoginView.as_view(template_name='website/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(next_page='/'), name='logout'),
]
