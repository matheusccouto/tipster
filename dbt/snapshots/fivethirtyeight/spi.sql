{% snapshot spi_snapshot %}

    select * except (loaded_at) from {{ source('fivethirtyeight', 'spi') }}

{% endsnapshot %}
