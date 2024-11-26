insert into {{table.name}} (
    {%- for c in table.columns -%} 
    {{c.quoted_name()}}{{ ", " if not loop.last else "" }}
    {%- endfor -%})
values ({% for c in table.columns%}?{{ ", " if not loop.last else "" }}{% endfor %})