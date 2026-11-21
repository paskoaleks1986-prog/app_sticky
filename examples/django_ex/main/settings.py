"""Minimal Django settings: no database, no auth, no admin, no templates."""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "django-insecure-example-only"  # example app, never use in production
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS: list[str] = []

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "main.urls"

DATABASES: dict[str, dict[str, str]] = {}

USE_TZ = True
