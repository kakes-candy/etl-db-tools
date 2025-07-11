from abc import ABC
from jinja2 import Environment, PackageLoader


def sql_render(template: str, data) -> str:
    env = Environment(loader=PackageLoader("etl_db_tools", "templates"))

    template = env.get_template(template)

    return template.render(data=data)


"""
Object dat de definitie van een tabel kan opslaan. Bedoeld als interface
tussen verschillende bronnen. 
"""


class Column(ABC):
    def __init__(
        self,
        name: str,
        type: str,
        nullable: bool,
        length: int = None,
        precission: int = None,
        scale=None,
        default=None,
    ) -> None:
        self.name = name
        self.type = type
        self.nullable = nullable
        self.length = length
        self.precission = precission
        self.scale = scale
        self.default = default

    def to_sql(self) -> str:
        nullpart = None if self.nullable else "not null"

        match self.type:
            case "int" | "tinyint" | "bigint" | "bit":
                defaultpart = (
                    None if self.default is None else f"default (({str(self.default)}))"
                )
                sql = " ".join(
                    [
                        x
                        for x in [self.name, self.type, nullpart, defaultpart]
                        if x is not None
                    ]
                )

            case "uniqueidentifier":
                defaultpart = (
                    None if self.default is None else f"default '{str(self.default)}'"
                )
                sql = " ".join(
                    [
                        x
                        for x in [self.name, self.type, nullpart, defaultpart]
                        if x is not None
                    ]
                )

            case "date" | "datetime" | "datetime2" | "smalldatetime":
                # quote naked date(times)
                if self.default is None:
                    defaultpart = None
                elif self.default not in ["getdate()"]:
                    defaultpart = f"default '{self.default}'"
                else:
                    defaultpart = (
                        None if self.default is None else f"default {str(self.default)}"
                    )
                sql = " ".join(
                    [
                        x
                        for x in [self.name, self.type, nullpart, defaultpart]
                        if x is not None
                    ]
                )

            case "decimal":
                defaultpart = (
                    None if self.default is None else f"default (({str(self.default)}))"
                )
                type_complete = f"{self.type}({self.precission},{self.scale})"
                sql = " ".join(
                    [
                        x
                        for x in [self.name, type_complete, nullpart, defaultpart]
                        if x is not None
                    ]
                )

            case "float":
                defaultpart = (
                    None if self.default is None else f"default (({str(self.default)}))"
                )
                if self.precission is not None:
                    type_complete = f"float({self.precission})"
                else:
                    type_complete = "float"
                sql = " ".join(
                    [
                        x
                        for x in [self.name, type_complete, nullpart, defaultpart]
                        if x is not None
                    ]
                )

            case "nvarchar" | "nchar" | "char" | "varchar":
                defaultpart = (
                    None if self.default is None else f"default ('{self.default}')"
                )
                if self.length == -1 or self.length > 4000:
                    type_complete = f"{self.type}(max)"
                else:
                    type_complete = f"{self.type}({self.length})"
                sql = " ".join(
                    [
                        x
                        for x in [self.name, type_complete, nullpart, defaultpart]
                        if x is not None
                    ]
                )

            case _:
                raise ValueError(f"Data type not implemented: {self.type}")

        return sql.strip()

    def quoted_name(self):
        pass

    def __str__(self) -> str:
        return f"column: name: {self.name}, type: {self.type}, length: {self.length or ''}, precission: {self.precission or ''}, scale: {self.scale or ''}, default: {self.default or ''}"


class BaseTable(ABC):
    def __init__(self, name, columns: list[Column] = None) -> None:
        self.name = name
        self.columns = []
        if columns:
            for col in columns:
                self.add_column(col)

    def column_names(self) -> list[str]:
        return [x.name for x in self.columns]

    def add_column(self, column: Column) -> None:
        print(f"this column is an Column instance {isinstance(column, Column)}")
        if isinstance(column, Column):
            self.columns.append(column)
        else:
            raise TypeError("columns must be instance of class column")

    def drop_column(self, column_name) -> None:
        self.columns = [x for x in self.columns if x.name != column_name]

    def create_table_statement(self) -> None:
        return sql_render("create_table.sql", self)

    def __str__(self) -> str:
        return (
            f'table: {self.name}, columns: {", ".join([x.name for x in self.columns]) }'
        )
