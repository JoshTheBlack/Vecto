import json
from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.db.models import Q, F, BooleanField, ExpressionWrapper
from django.utils.html import format_html, mark_safe
from django.urls import reverse
from .models import Network, PatreonTier, Podcast, Episode, UserMix, PatronProfile, EpisodeEditSuggestion

class S3SubscriberAudioFilter(SimpleListFilter):
    title = 'S3 Hosted Audio (Affected)'
    parameter_name = 's3_audio'

    def lookups(self, request, model_admin):
        return (
            ('yes', 'Hosted on Amazon S3'),
            ('no', 'Hosted Elsewhere'),
        )

    def queryset(self, request, queryset):
        # Using icontains catches both s3.amazonaws.com/bucket and bucket.s3.amazonaws.com
        if self.value() == 'yes':
            return queryset.filter(audio_url_subscriber__icontains='s3.amazonaws.com')
        if self.value() == 'no':
            return queryset.exclude(audio_url_subscriber__icontains='s3.amazonaws.com')
        return queryset
    
@admin.register(Network)
class NetworkAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug')
    search_fields = ('name',)
    filter_horizontal = ('owners',)

@admin.register(Episode)
class EpisodeAdmin(admin.ModelAdmin):
    list_display = ('title', 'podcast', 'pub_date', 'match_reason', 'is_metadata_locked', 'has_public_audio', 'has_premium_audio')
    list_filter = ('podcast__network', 'podcast', S3SubscriberAudioFilter, 'pub_date', 'match_reason')
    search_fields = ('title', 'raw_description', 'guid_public', 'guid_private')
    list_editable = ('is_metadata_locked',) # Allows you to check the box directly from the list view!
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        # Calculate the boolean logic inside the database so the admin table can sort it
        qs = qs.annotate(
            _has_public=ExpressionWrapper(
                Q(audio_url_public__isnull=False) & ~Q(audio_url_public=''),
                output_field=BooleanField()
            ),
            _has_premium=ExpressionWrapper(
                Q(audio_url_subscriber__isnull=False) & ~Q(audio_url_subscriber='') & ~Q(audio_url_subscriber=F('audio_url_public')),
                output_field=BooleanField()
            )
        )
        return qs

    def has_public_audio(self, obj):
        return bool(obj.audio_url_public)
    has_public_audio.boolean = True
    has_public_audio.short_description = "Public"
    has_public_audio.admin_order_field = '_has_public'  # Links the column to the SQL annotation

    def has_premium_audio(self, obj):
        return bool(obj.audio_url_subscriber) and obj.audio_url_subscriber != obj.audio_url_public
    has_premium_audio.boolean = True
    has_premium_audio.short_description = "Premium"
    has_premium_audio.admin_order_field = '_has_premium'  # Links the column to the SQL annotation
    
    ordering = ('-pub_date',)

@admin.register(PatronProfile)
class PatronProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'patreon_id', 'get_combined_pledge_cents', 'last_sync')
    search_fields = ('user__username', 'user__email', 'patreon_id')

    def get_combined_pledge_cents(self, obj):
        """Sums up all campaign pledges from the active_pledges JSON field."""
        if not obj.active_pledges:
            return 0
        return sum(obj.active_pledges.values())
    
    get_combined_pledge_cents.short_description = 'Pledge Amount Cents'

class PatronProfileInline(admin.StackedInline):
    model = PatronProfile
    can_delete = False
    verbose_name_plural = 'Patron Profile'
    
    # We leave 'active_pledges' editable, but add our helper guide as read-only
    fields = ('patreon_id', 'feed_token', 'last_active', 'campaign_reference_guide', 'active_pledges')
    readonly_fields = ('campaign_reference_guide', 'feed_token', 'last_active')

    def campaign_reference_guide(self, obj):
        networks = Network.objects.exclude(patreon_campaign_id__isnull=True).exclude(patreon_campaign_id='')
        
        # 1. Build the Interactive HTML UI
        html = "<div style='padding: 15px; background-color: #212529; color: #f8f9fa; border-radius: 6px; border: 1px solid #495057;'>"
        html += "<h4 style='color: #ffc107; margin-top: 0;'><i class='bi bi-sliders'></i> Patreon Test Interface</h4>"
        html += "<p style='font-size: 0.9em; color: #adb5bd;'>Enter pledge amounts below in dollars (e.g. 5.00). The hidden JSON database field will update automatically.</p>"
        
        for net in networks:
            camp_id = str(net.patreon_campaign_id)
            current_cents = obj.active_pledges.get(camp_id, 0) if obj and obj.active_pledges else 0
            current_dollars = current_cents / 100.0
            
            html += f"<div style='margin-bottom: 10px; padding-bottom: 10px; border-bottom: 1px solid #343a40; display: flex; justify-content: space-between; align-items: center;'>"
            html += f"  <div><strong style='font-size: 1.1em;'>{net.name}</strong><br><span style='color: #6c757d; font-size: 0.85em;'>ID: {camp_id}</span></div>"
            html += f"  <div style='display: flex; align-items: center;'>$"
            html += f"    <input type='number' step='0.01' min='0' class='custom-pledge-input' data-campaign-id='{camp_id}' value='{current_dollars:.2f}' "
            html += f"           style='margin-left: 5px; width: 100px; padding: 4px; border-radius: 4px; border: 1px solid #6c757d; background: #343a40; color: white;'>"
            html += f"  </div>"
            html += f"</div>"

        # --- NEW: RAW JSON DEBUG BLOCK ---
        raw_json_str = json.dumps(obj.active_pledges, indent=2) if obj and obj.active_pledges else "{}"
        html += "<div style='margin-top: 15px; padding-top: 10px; border-top: 1px dashed #495057;'>"
        html += "<div style='font-size: 0.75em; color: #6c757d; margin-bottom: 5px; text-transform: uppercase; letter-spacing: 1px;'>Raw JSON Payload (Live)</div>"
        html += f"<pre id='raw-json-debug' style='background: #1a1d20; color: #6c757d; padding: 10px; border-radius: 4px; font-size: 0.85em; overflow-x: auto; margin: 0;'>{raw_json_str}</pre>"
        html += "</div>"
        # ---------------------------------
            
        html += "</div>"

        # 2. Inject the Javascript to act as the "Glue"
        js = """
        <script>
        document.addEventListener('DOMContentLoaded', function() {
            // Find the actual Django JSON textarea and our new debug block
            const jsonField = document.querySelector('textarea[id$="-active_pledges"]');
            const debugBlock = document.getElementById('raw-json-debug');
            if (!jsonField) return;

            // Hide the original field's entire row to keep the UI clean
            const fieldRow = jsonField.closest('.form-row');
            if (fieldRow) {
                fieldRow.style.display = 'none';
            }

            // Grab all our new custom inputs
            const inputs = document.querySelectorAll('.custom-pledge-input');

            // Function to compile the inputs and inject them into both locations
            function updateJSON() {
                const newData = {};
                inputs.forEach(input => {
                    const val = parseFloat(input.value);
                    if (!isNaN(val) && val > 0) {
                        newData[input.dataset.campaignId] = Math.round(val * 100); 
                    }
                });
                
                // 1. Update the hidden field for Django to save
                jsonField.value = JSON.stringify(newData);
                
                // 2. Update the visual debug block with pretty-printed JSON
                if (debugBlock) {
                    debugBlock.textContent = JSON.stringify(newData, null, 2);
                }
            }

            // Listen for keystrokes/clicks on our custom inputs
            inputs.forEach(input => {
                input.addEventListener('input', updateJSON);
            });
        });
        </script>
        """
        
        return mark_safe(html + js)

admin.site.unregister(User)

@admin.register(User)
class CustomUserAdmin(BaseUserAdmin):
    inlines = (PatronProfileInline,)
    
    # Append BOTH of our custom columns to the end of the standard Django list
    list_display = BaseUserAdmin.list_display + ('get_total_pledge_display', 'impersonate_action')

    # --- Column 1: Patreon Pledge ---
    def get_total_pledge_display(self, instance):
        if hasattr(instance, 'patron_profile') and instance.patron_profile.active_pledges:
            total_cents = sum(instance.patron_profile.active_pledges.values())
            return f"${total_cents / 100:.2f}"
        return "$0.00"
    
    get_total_pledge_display.short_description = 'Total Pledge'

    # --- Column 2: Impersonation ---
    def impersonate_action(self, obj):
        if not obj.is_superuser:
            url = reverse('start_impersonation', args=[obj.id])
            # format_html is correct here because we are injecting the 'url' variable
            return format_html('<a class="button" style="background-color: #ffc107; color: black; font-weight: bold; padding: 5px 10px; border-radius: 4px;" href="{}">Impersonate</a>', url)
        
        # mark_safe is required here because there are no variables to inject
        return mark_safe('<span style="color: gray;">Superuser (Locked)</span>')
        
    impersonate_action.short_description = 'Impersonate User'

 
admin.site.register(PatreonTier)
admin.site.register(Podcast)
admin.site.register(UserMix)

@admin.register(EpisodeEditSuggestion)
class EpisodeEditSuggestionAdmin(admin.ModelAdmin):
    # What shows up in the main list view
    list_display = ('episode', 'user', 'status', 'is_first_responder', 'created_at')
    
    # Adds a sidebar to filter by pending/approved or first responders
    list_filter = ('status', 'is_first_responder', 'created_at')
    
    # Adds a search bar to find specific users or episodes
    search_fields = ('episode__title', 'user__username', 'user__email')
    
    # Protects the timestamps from accidental editing
    readonly_fields = ('created_at', 'resolved_at')
    
    # Organizes the detail page layout
    fieldsets = (
        ('Reference', {
            'fields': ('episode', 'user', 'status', 'is_first_responder')
        }),
        ('Payload Data (JSON)', {
            'fields': ('suggested_data', 'original_data')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'resolved_at')
        }),
    )