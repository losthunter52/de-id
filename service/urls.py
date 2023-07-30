from django.urls import path
from . import views

urlpatterns = [
    path('anonymize', views.anonymize, name='anonymize'),
    path('result', views.result, name='result'),
    path('register', views.register, name='register'),  
    path('login', views.login, name='login'),         
]
