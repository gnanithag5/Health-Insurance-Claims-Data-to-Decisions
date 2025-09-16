{% snapshot provider_snapshots %}
{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='provider',
      strategy='check',
      check_cols=['potentialfraud']
    )
}}

select
    provider,
    potentialfraud
from {{ source('source_raw_data', 'provider_raw') }}

{% endsnapshot %}
