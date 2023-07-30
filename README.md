
# Running the api

## Terminal 1
```bash
  [...]
    cd de-id
    python -m venv venv
    venv/scripts/activate
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    python manage.py makemigrations
    python manage.py migrate
    python manage.py runserver
  [...]
```

## Terminal 2
```bash
  [...]
    cd de-id
    python -m venv celery_venv
    celery_venv/scripts/activate
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    celery -A config worker --pool=solo -l info
  [...]
```
