import dataset
import dataset.types
import datetime
import importlib
import pathlib
import typing
import yaml

# ==================================================================================================


class Database:
    """
    TODO
    """

    def __init__(self, db_host_url: str, db_name: str) -> None:
        """
        TODO
        """
        print(f"connect to {db_host_url}/{db_name}")
        self.db: dataset.Database = dataset.connect(
            url=f"{db_host_url}/{db_name}",
            ensure_schema=False
        )

    def create_table_from_yml(self, path: pathlib.Path, drop_if_exists: bool) -> dataset.Table:
        """
        TODO
        """
        assert path, "no path to yml file given"

        # Load the yml file.
        with path.open() as stream:
            yml = yaml.safe_load(stream)
        print(yml)

        class_path = yml["class"]
        columns = yml["columns"]

        # Import the specified class.
        # For example, for entry 'model: pigeon.models.User', import 'pigeon.models.User'.
        class_path_components = class_path.split(".")
        module_name = ".".join(class_path_components[:-1])  # pigeon.models
        class_name = class_path_components[-1]  # User
        module = importlib.import_module(module_name)
        clazz = getattr(module, class_name)
        class_type_hints = typing.get_type_hints(clazz)

        # If a table with the given name already exists, drop it.
        table: dataset.Table = self.get_table(class_path)
        if table:
            if drop_if_exists:
                table.drop()
            else:
                return table

        # Create the table and its columns.
        table = self.db.create_table(class_path)
        for name, column_spec in columns:
            type = self._map_type(column_spec.get("type", class_type_hints[name]))
            primary_key = column_spec.get("is_primary_key", False)
            unique = column_spec.get("unique", False)
            nullable = column_spec.get("nullable", False)
            autoincrement = column_spec.get("autoincrement", False)
            default = column_spec.get("default", False)

            table.create_column(
                name=name,
                type=type,
                primary_key=primary_key,
                unique=unique,
                nullable=nullable,
                autoincrement=autoincrement,
                default=default,
            )

        # Create a column for storing the creation and modification date of a row.
        table.create_column("ts_created", dataset.types.DateTime)
        table.create_column("ts_modified", dataset.types.DateTime)

        # Create the indexes.
        indexes = yml.get("indexes")
        if indexes:
            for index_name, index_spec in indexes:
                columns = index_spec["columns"]
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
        for table in self.db.tables:
            self.db[table].drop()

    # ----------------------------------------------------------------------------------------------

    def insert_one(self, object: typing.Any) -> None:
        """
        TODO
        """
        assert object is not None, "no object given"

        table = self.get_table(object)
        if table is None:
            raise ValueError("no table exists for storing the object'")

        row = vars(object)
        row["ts_created"] = row["ts_modified"] = datetime.datetime.utcnow()

        table.insert(row, ensure=False)

    # ----------------------------------------------------------------------------------------------

    def get_table(self, obj: typing.Any) -> dataset.Table:
        """
        TODO
        """
        table_name = obj.__name__ if isinstance(obj, type) else type(obj).__name__
        return self.db[table_name] if self.db.has_table(table_name) else None

    def _map_type(self, type: type) -> dataset.types:
        """
        TODO
        """
        if type is str:
            return dataset.types.String
        if type is bool:
            return dataset.types.Boolean
        if type is int:
            return dataset.types.BigInteger
        if type is float:
            return dataset.types.Float
        if type is datetime:
            return dataset.types.DateTime
        if type is dict:
            return dataset.types.JSONB

        return dataset.types.String

# ==================================================================================================


class DatabasePool:
    """
    TODO
    """

    def __init__(self, db_host_url: str):
        """
        TODO
        """
        self.db_host_url: str = db_host_url
        self.db_cache: dict[str, Database] = {}

    def create_database(self, db_name: str, drop_if_exists: bool) -> Database | None:
        """
        TODO
        """
        print("CREATE DATABASE")
        print(f"host: {self.db_host_url}")
        print(f"name: {db_name}")
        if db_name in self.db_cache:
            if drop_if_exists:
                print("DB already exists. Drop.")
                self.db_cache.pop(db_name).drop()
            else:
                print("DB already exists.")
                return self.db_cache[db_name]

        print("Creating new DB.")
        self.db_cache[db_name] = Database(self.db_host_url, db_name)

        return self.db_cache.get(db_name)

    def get_database(self, db_name: str, create_if_not_exists: bool = False) -> Database | None:
        """
        TODO
        """
        if db_name not in self.db_cache and create_if_not_exists:
            self.db_cache[db_name] = Database(self.db_host_url, db_name)

        return self.db_cache.get(db_name)
