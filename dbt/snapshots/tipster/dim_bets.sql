{% snapshot dim_bets_snapshot %}

select * from {{ ref('dim_bets') }}

{% endsnapshot %}