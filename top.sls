base:
  '*':
    - sysconfig.dir
    - sysconfig.hostname
    - java


{% for module, role in grains['roles'].iteritems() %}
  {% if module != 'salt' %}
    - {{module}}
  {% endif %}
  
  {# spark1 config #}
  {% if module == 'spark1' %}
    - spark1.conf
    - hadoop2.conf
  {% endif %}
{% endfor %}
