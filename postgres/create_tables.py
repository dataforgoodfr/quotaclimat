import logging

from postgres.schemas.models import Sitemap

def config_tables():
    """List of table schemas files"""
    return ["postgres/schemas/sitemap.pgsql"]
