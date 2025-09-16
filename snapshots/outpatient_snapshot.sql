{% snapshot outpatient_snapshots %}
{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='claimid',
      strategy='check',
      check_cols=['ClmDiagnosisCode_2']
    )
}}

select
    claimid,
    beneid,
    ClmDiagnosisCode_2
from {{ source('source_raw_data', 'outpatient_raw') }}

{% endsnapshot %}
