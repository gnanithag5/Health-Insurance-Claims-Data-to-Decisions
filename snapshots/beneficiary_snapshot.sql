{% snapshot beneficiary_snapshots %}
{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='beneid',
      strategy='check',
      check_cols=['NoOfMonths_PartACov', 'ChronicCond_Diabetes']
    )
}}

select
    beneid,
    NoOfMonths_PartACov,
    ChronicCond_Diabetes
from {{ source('source_raw_data', 'beneficiary_raw') }}

{% endsnapshot %}
