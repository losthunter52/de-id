from rest_framework.decorators import api_view, parser_classes, authentication_classes, permission_classes
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from .tasks import process_data
from rest_framework.authtoken.models import Token
from django.contrib.auth.models import User
from django.contrib.auth import authenticate
from .models import Task

def check_required_fields(data, fields):
    """
    Check if the required fields are present in the given data.

    Args:
        data (dict): The data dictionary to be checked.
        fields (list): List of field names to be checked for presence.

    Return:
        bool: True if all required fields are present, False otherwise.
    """
    for field in fields:
        if not data.get(field):
            return False
    return True

@api_view(['POST'])
@parser_classes([JSONParser])
def register(request):
    """
    Endpoint to register a new user and return an authentication token.

    Args:
        request (rest_framework.request.Request): The HTTP request object.

    Return:
        rest_framework.response.Response: The HTTP response object containing the token or error message.
    """
    data = request.data
    username = data.get('username')
    password = data.get('password')

    if not check_required_fields(data, ['username', 'password']):
        return Response({"error": "Username and password are required."}, status=400)

    # Check if the user already exists
    if User.objects.filter(username=username).exists():
        return Response({"error": "Username already exists."}, status=400)

    # Create a new user
    user = User.objects.create_user(username=username, password=password)

    # Create a token for the new user
    token = Token.objects.create(user=user)

    return Response({"token": token.key}, status=201)


@api_view(['POST'])
@parser_classes([JSONParser])
def login(request):
    """
    Endpoint to perform login and return an authentication token.

    Args:
        request (rest_framework.request.Request): The HTTP request object.

    Return:
        rest_framework.response.Response: The HTTP response object containing the token or error message.
    """
    data = request.data
    username = data.get('username')
    password = data.get('password')

    if not check_required_fields(data, ['username', 'password']):
        return Response({"error": "Username and password are required."}, status=400)

    # Authenticate the user
    user = authenticate(username=username, password=password)

    if not user:
        return Response({"error": "Invalid username or password."}, status=401)

    # Get or create a token for the authenticated user
    token, _ = Token.objects.get_or_create(user=user)

    return Response({"token": token.key}, status=200)


# Use the list of decorators to group and avoid repetitions
@api_view(['POST'])
@parser_classes([JSONParser])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def anonymize(request):
    """
    Endpoint to initiate an asynchronous anonymization task.

    Args:
        request (rest_framework.request.Request): The HTTP request object.

    Return:
        rest_framework.response.Response: The HTTP response object containing the task status and ID.
    """
    data = request.data


    # Call the Celery asynchronous task
    task_result = process_data.delay(data, request.user.pk)

    response = {
        "message": "Anonymization task has been scheduled."
    }

    # Return a response confirming the request
    return Response(response, status=202)

@api_view(['GET'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def result(request):
    """
    Endpoint to retrieve the result of an anonymization task.

    Args:
        request (rest_framework.request.Request): The HTTP request object.
        task_id (str): The ID of the anonymization task.

    Return:
        rest_framework.response.Response: The HTTP response object containing the task result or status.
    """
    # Implement the logic to retrieve the anonymization task result
    # based on the provided task_id and return it as a JSON response
    result = []  # Logic to obtain the result based on task_id

    user = request.user
    tasks = Task.objects.filter(user=user).order_by('-creation_date')

    for task in tasks:
        if task.status == "COMPLETED_WITH_ERRORS":
            result.append({"created_at": str(task.creation_date), "status": str(task.status), "errors": str(task.errors), "results": str(task.result)})
        else:
            result.append({"created_at": str(task.creation_date), "status": str(task.status), "results": str(task.result)})

    return Response({"result": result})
