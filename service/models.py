# models.py

from django.db import models
from django.contrib.auth.models import User

class Task(models.Model):
    TASK_STATUS_CHOICES = (
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
    )

    task_id = models.CharField(max_length=100, unique=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    status = models.CharField(max_length=100, default='PENDING')
    errors = models.TextField(blank=True, null=True)
    result = models.TextField(blank=True, null=True)
    creation_date = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Task ID: {self.task_id}, User: {self.user.username}, Status: {self.status}"
