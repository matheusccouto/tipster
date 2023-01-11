{% snapshot odds_snapshot %}

    select * except (loaded_at) from {{ source('theoddsapi', 'odds') }}

{% endsnapshot %}
