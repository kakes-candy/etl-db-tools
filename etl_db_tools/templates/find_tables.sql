
select concat(sc.name, '.', s.[name]) as table_name, object_id
from sys.tables as s
inner join sys.schemas as sc
on s.schema_id = sc.schema_id
where 1=1
    and sc.name = '{{data.schema}}'
    {% if data.startswith %}
    and s.name like '{{data.startswith}}%'
    {% endif %}
    and sc.name = '{{data.schema}}'
    {% if data.contains %}
    and s.name like '%{{data.contains}}%'
    {% endif %}       
    and s.[type] = 'U'
order by s.object_id