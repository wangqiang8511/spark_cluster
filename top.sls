base:
  '*':
    - base
    - sysconfig.dir
    - sysconfig.hostname
    - java
    - java.env


{% for module, role in grains['roles'].iteritems() %}
    - {{module}}
  
  {# spark1 config #}
  {% if module == 'spark1' %}
    - spark1.conf
    - hadoop2.conf
  {% endif %}
{% endfor %}
