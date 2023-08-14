from django.urls import path
from . import views

urlpatterns = [
    path('anonymize', views.anonymize, name='anonymize'),
    path('results', views.results, name='results'),
    path('result_detail/<str:task_id>', views.result_detail, name='result_detail'),
    path('register', views.register, name='register'),  
    path('login', views.login, name='login'),         
]
