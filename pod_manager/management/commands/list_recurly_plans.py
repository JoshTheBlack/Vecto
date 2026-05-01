import recurly
from django.conf import settings
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Queries the Recurly API to list all active subscription plans'

    def handle(self, *args, **options):
        api_key = getattr(settings, 'RECURLY_API_KEY', None)
        
        if not api_key:
            self.stdout.write(self.style.ERROR("RECURLY_API_KEY is not set in settings.py or .env."))
            return

        self.stdout.write("Connecting to Recurly API...")

        try:
            client = recurly.Client(api_key)
            
            # Use the 'params' dictionary for filtering by state
            plan_items = client.list_plans(params={'state': 'active'}).items()
            
            # Use a list comprehension to drain the pager safely
            plan_list = [p for p in plan_items]
            
            if not plan_list:
                self.stdout.write(self.style.WARNING("No active plans found."))
                return

            self.stdout.write(self.style.SUCCESS(f"Found {len(plan_list)} Active Plans:\n"))
            
            header = f"{'Plan Name':<30} | {'Plan Code':<20} | {'ID':<20}"
            self.stdout.write(header)
            self.stdout.write("-" * len(header))

            for plan in plan_list:
                self.stdout.write(f"{plan.name[:30]:<30} | {plan.code:<20} | {plan.id:<20}")

        except recurly.ApiError as e:
            # Corrected exception handling for Recurly v3 SDK
            self.stdout.write(self.style.ERROR(f"Recurly API Error: {e.message}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An unexpected error occurred: {str(e)}"))