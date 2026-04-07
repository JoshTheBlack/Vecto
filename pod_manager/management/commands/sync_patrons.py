import requests
from django.core.management.base import BaseCommand
from django.conf import settings
from pod_manager.models import PatronProfile
from pod_manager.views import get_bald_move_pledge_amount # Reuse your existing logic!

class Command(BaseCommand):
    help = 'Syncs all PatronProfile pledge amounts with the Patreon API'

    def handle(self, *args, **options):
        profiles = PatronProfile.objects.all()
        self.stdout.write(f"Starting sync for {profiles.count()} profiles...")

        # We need a valid creator access token to fetch identity data for others
        # For now, we'll use the user's stored token if available, 
        # but in production, you'd use a long-lived Creator Token.
        for profile in profiles:
            self.stdout.write(f"Syncing {profile.user.email}...")
            
            # This URL fetches the member data for the authenticated user
            user_url = (
                "https://www.patreon.com/api/oauth2/v2/identity"
                "?include=memberships,memberships.campaign"
                "&fields[user]=email"
                "&fields[member]=patron_status,currently_entitled_amount_cents"
            )
            
            # NOTE: This requires the user to have a valid, non-expired token.
            # We'll refine the Token Refresh logic in a later step.
            headers = {'Authorization': f'Bearer {settings.PATREON_CREATOR_TOKEN}'}
            
            try:
                response = requests.get(user_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    new_amount = get_bald_move_pledge_amount(data)
                    
                    if profile.pledge_amount_cents != new_amount:
                        self.stdout.write(self.style.SUCCESS(
                            f"Updated {profile.user.email}: {profile.pledge_amount_cents} -> {new_amount}"
                        ))
                        profile.pledge_amount_cents = new_amount
                        profile.save()
                else:
                    self.stdout.write(self.style.ERROR(f"Failed to sync {profile.user.email}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))

        self.stdout.write(self.style.SUCCESS("Sync complete."))