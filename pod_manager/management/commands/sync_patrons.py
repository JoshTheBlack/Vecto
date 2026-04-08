import requests
from django.core.management.base import BaseCommand
from django.conf import settings
from pod_manager.models import PatronProfile, Network
from pod_manager.views import get_active_pledge_amount

class Command(BaseCommand):
    help = 'Syncs all PatronProfile pledge amounts with the Patreon API'

    def handle(self, *args, **options):
        profiles = PatronProfile.objects.all()
        primary_network = Network.objects.first()
        campaign_id = primary_network.patreon_campaign_id if primary_network else None
        
        self.stdout.write(f"Starting sync for {profiles.count()} profiles...")

        for profile in profiles:
            self.stdout.write(f"Syncing {profile.user.email}...")
            
            user_url = (
                "https://www.patreon.com/api/oauth2/v2/identity"
                "?include=memberships,memberships.campaign"
                "&fields[user]=email"
                "&fields[member]=patron_status,currently_entitled_amount_cents"
            )
            
            headers = {'Authorization': f'Bearer {settings.PATREON_CREATOR_TOKEN}'}
            
            try:
                response = requests.get(user_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    new_amount = get_active_pledge_amount(data, campaign_id)
                    
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