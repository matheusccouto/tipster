{% snapshot spi_snapshot %}

{{
    config(
          target_schema='fivethirtyeight',
          strategy='check',
          unique_key="season||'-'||league_id||'-'||team1||'-'||team2",
          check_cols='all',
    )
}}

select * except (loaded_at) from {{ source('fivethirtyeight', 'spi') }}

{% endsnapshot %}