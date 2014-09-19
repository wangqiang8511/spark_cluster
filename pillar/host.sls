{% set mid = grains['id'].split('.') %}
hostname: {{mid[0]}}
cluster: {{mid[1]}}
domain: {{'.'.join(mid[1:])}}
