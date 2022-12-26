{% snapshot fct_h2h_snapshot %}

    select * from {{ ref('fct_h2h') }}

{% endsnapshot %}
