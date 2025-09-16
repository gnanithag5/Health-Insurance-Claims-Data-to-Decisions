{% snapshot inpatient_snapshots %}
{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='claimid',
      strategy='check',
      check_cols=['CLMDIAGNOSISCODE_1']
    )
}}

select
    claimid,
    beneid,
    CLMDIAGNOSISCODE_1
from {{ source('source_raw_data', 'inpatient_raw') }}

{% endsnapshot %}
