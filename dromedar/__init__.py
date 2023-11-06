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

    def create_table_from_yml(self, path: str, drop_if_exists: bool) -> dataset.Table:
        """
        TODO
        """
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

        # Import the specified class.
        # For example, for entry 'class: pigeon.models.User', import 'pigeon.models.User'.
        class_path_elements = class_path.split(".")
        module_name = ".".join(class_path_elements[:-1])  # pigeon.models
        class_name = class_path_elements[-1]              # User
        module = importlib.import_module(module_name)
        clazz = getattr(module, class_name)
        class_type_hints = typing.get_type_hints(clazz)

        # Ensure that the key of each 'columns' entry is an attribute of the class.
        for key in columns.keys():
            if not hasattr(clazz, key):
                raise ValueError(f"Class {class_path} has no attribute '{key}'.")

        # If a table for storing objects of the specified class already exists, drop it.
        table: dataset.Table = self.get_table(class_name)
        if table:
            if drop_if_exists:
                table.drop()
            else:
                return table

        # Create the table and the columns.
        table = self.db.create_table(class_name)
        for column_name, column_spec in columns.items():
            # If the entry does not contain a column_spec, use the default values.
            column_spec = column_spec or {}

            # If the column_spec doesn't provide a type, use the type specified in the class.
            type = self.map_type(column_spec.get("type", class_type_hints[column_name]))

            primary_key = column_spec.get("primary_key", False)
            unique = column_spec.get("unique", False)
            nullable = column_spec.get("nullable", True)
            autoincrement = column_spec.get("autoincrement", False)
            default = column_spec.get("default")

            table.create_column(
                name=column_name,
                type=type,
                primary_key=primary_key,
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

                postgresql_using = index_spec.get("postgresql_using")
                postgresql_ops = index_spec.get("postgresql_ops")

                table.create_index(
                    name=index_name,
                    columns=columns,
                    postgresql_using=postgresql_using,
                    postgresql_ops=postgresql_ops,
                )

        return table

    def drop(self) -> None:
        """
        TODO
        """
        self.db.drop()

    # ----------------------------------------------------------------------------------------------

    def insert_one(self, object: typing.Any) -> None:
        """
        TODO
        """
        assert object is not None, "no object given"

        table = self.get_table(object)
        if table is None:
            raise ValueError(f"no table exists for storing an object of type '{type(object)}'.")

        row = vars(object)
        row["ts_created"] = row["ts_modified"] = datetime.datetime.utcnow()

        table.insert(row, ensure=False)

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
