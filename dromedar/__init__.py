import dataset
import dataset.types
import datetime
import importlib
import typing
import yaml

# ==================================================================================================


class Database:
    """
    TODO
    """

    def __init__(self, db_host_url: str, db_name: str, create_if_not_exists: bool = True) -> None:
        """
        TODO
        """
        self.db: dataset.Database = dataset.connect(
            url=f"{db_host_url}/{db_name}",
            create_if_not_exists=create_if_not_exists,
            ensure_schema=False
        )

    def create_table_from_yml(
            self,
            clazz: type,
            path: str,
            drop_if_exists: bool = True) -> dataset.Table:
        """
        TODO
        """
        assert clazz, "no class given"
        assert path, "no path to yml file given"

        # Load the yml file.
        with open(path, "r") as stream:
            yml = yaml.safe_load(stream)

        class_path = yml.get("class")
        if not class_path:
            raise ValueError(f"The yml file '{path}' does not contain a 'class' entry.")

        columns = yml.get("columns")
        if not columns:
            raise ValueError(f"The yml file '{path}' does not contain a 'columns' entry.")
        if not isinstance(columns, dict):
            raise ValueError(f"Wrong format of the 'columns' entry in yml file '{path}'.")

        # Iterate through each 'columns' entry to ensure that it is an attribute of the class
        # and to identify the primary key.
        type_hints = typing.get_type_hints(clazz)
        primary_key: str = None
        for key in columns.keys():
            if key not in type_hints:
                raise ValueError(f"Class {class_path} has no attribute '{key}'.")

            column_spec = columns.get(key) or {}
            if column_spec.get("primary_key") is True:
                primary_key = key

        # If a table for storing objects of the specified class already exists, drop it.
        table_name = clazz.__name__
        table: dataset.Table = self.get_table(table_name)
        if table:
            if drop_if_exists:
                table.drop()
            else:
                return table

        # Create the table and the columns.
        table = self.db.create_table(
            table_name,
            primary_id=primary_key,
            primary_type=self.map_type(columns[primary_key].get("type", type_hints[primary_key]))
        )
        for column_name, column_spec in columns.items():
            # If the entry does not contain a column_spec, use the default values.
            column_spec = column_spec or {}

            # If the column_spec doesn't provide a type, use the type specified in the class.
            type = self.map_type(column_spec.get("type", type_hints[column_name]))

            is_primary_key = column_spec.get("primary_key", False)
            unique = column_spec.get("unique", False)
            nullable = column_spec.get("nullable", True)
            autoincrement = column_spec.get("autoincrement", False)
            default = column_spec.get("default")

            table.create_column(
                name=column_name,
                type=type,
                primary_key=is_primary_key,
                unique=unique,
                nullable=nullable,
                autoincrement=autoincrement,
                default=default,
            )

        # Create two extra columns, for storing the creation and modification date.
        table.create_column("ts_created", dataset.types.DateTime)
        table.create_column("ts_modified", dataset.types.DateTime)

        # Create the indexes.
        indexes = yml.get("indexes")
        if indexes:
            for index_name, index_spec in indexes.items():
                # Ensure a 'columns' entry exists.
                columns = index_spec.get("columns")
                if not columns:
                    raise ValueError(f"No columns for index '{index_name}' specified.")

                kwargs = {}

                if "postgresql_using" in index_spec:
                    kwargs["postgresql_using"] = index_spec["postgresql_using"]

                if "postgresql_ops" in index_spec:
                    kwargs["postgresql_ops"] = index_spec["postgresql_ops"]

                table.create_index(name=index_name, columns=columns, **kwargs)

        return table

    # ----------------------------------------------------------------------------------------------

    def drop(self) -> None:
        """
        TODO
        """
        self.db.drop()

    def drop_tables(self) -> None:
        """
        TODO
        """
        for table in self.db.tables:
            self.db[table].drop()

    # ----------------------------------------------------------------------------------------------

    def insert_one(self, object: typing.Any) -> None:
        """
        TODO
        """
        assert object, "no object given"

        table = self.get_table(object)
        if table is None:
            raise ValueError(f"no table exists for storing an object of type '{type(object)}'.")

        row = vars(object)
        row["ts_created"] = row["ts_modified"] = datetime.datetime.utcnow()

        table.insert(row, ensure=False)

    def insert_many(self, objects: list[typing.Any]) -> None:
        """
        TODO
        """
        assert objects, "no objects given"

        table = self.get_table(objects[0])
        if table is None:
            raise ValueError(f"no table exists for storing objects of type '{type(objects[0])}'.")

        rows = []
        now = datetime.datetime.utcnow()
        for object in objects:
            row = vars(object)
            row["ts_created"] = row["ts_modified"] = now
            rows.append(row)

        table.insert_many(rows, ensure=False)

    # ----------------------------------------------------------------------------------------------

    def get_table(self, obj: typing.Any) -> dataset.Table:
        """
        TODO
        """
        assert obj, "no object given"

        table_name = obj.__name__ if isinstance(obj, type) else type(obj).__name__
        return self.db[table_name] if self.db.has_table(table_name) else None

    # ----------------------------------------------------------------------------------------------

    def map_type(self, type: type | str) -> dataset.types:
        """
        TODO
        """
        if type is str or type == "str":
            return dataset.types.String
        if type is bool or type == "bool":
            return dataset.types.Boolean
        if type is int or type == "int":
            return dataset.types.BigInteger
        if type is float or type == "float":
            return dataset.types.Float
        if type is datetime or type == "datetime":
            return dataset.types.DateTime
        if type is dict or type == "dict":
            return dataset.types.JSONB

        return dataset.types.String
