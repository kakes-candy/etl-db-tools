insert into {{data.name}} (
    {%- for c in data.columns -%} 
    {{c.quoted_name()}}{{ ", " if not loop.last else "" }}
    {%- endfor -%})
values ({% for c in data.columns%}?{{ ", " if not loop.last else "" }}{% endfor %})