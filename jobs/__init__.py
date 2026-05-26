from nautobot.apps.jobs import register_jobs
from .patch_manager_import_nautobot_job import PatchManagerImport

register_jobs(PatchManagerImport)
