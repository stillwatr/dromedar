import dataset
import dataset.types
import datetime
import logging
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
        assert db_host_url, "no db host url given"
        assert db_name, "no db name given"

        self.log = logging.getLogger("dromedar")
        self.log.debug(f"init db '{db_name}' | db_host_url: '{db_host_url}'")
        self.db: dataset.Database = dataset.connect(
            url=f"{db_host_url}/{db_name}",
            ensure_schema=False
        )

    def create_table_from_yml(self, clazz: type, path: str, drop: bool = True) -> dataset.Table:
        """
        TODO
        """
        assert clazz, "no class given"
        assert path, "no path to yml file given"

        self.log.debug(f"creating table from yml | class: '{clazz}', path: '{path}', drop: {drop}")

        # Load the yml file.
        with open(path, "r") as stream:
            yml = yaml.safe_load(stream)

        columns = yml.get("columns")
        if not columns:
            raise ValueError(f"yml file '{path}' does not contain a 'columns' entry")
        if not isinstance(columns, dict):
            raise ValueError(f"wrong format of the 'columns' entry in yml file '{path}'")

        # Iterate through all columns entries to find the primary key and to ensure that each
        # specified key is an attribute of the class.
        type_hints = typing.get_type_hints(clazz)
        primary_key = None
        for key in columns.keys():
            if key not in type_hints:
                raise ValueError(f"class '{clazz}' has no attribute '{key}'")

            column_spec = columns.get(key) or {}
            if column_spec.get("primary_key") is True:
                primary_key = key

        # Abort if no primary key is specified.
        if primary_key is None:
            raise ValueError("no primary key specified")

        # Obtain the type of the primary column.
        primary_type = self.map_type(columns[primary_key].get("type", type_hints[primary_key]))

        # Check if a table for storing objects of the class already exists. Drop it when necessary.
        table_name = clazz.__name__
        table: dataset.Table = self.get_table(table_name)
        if table:
            if drop:
                self.log.debug(f"dropping table '{table_name}' (already exists)")
                table.drop()
            else:
                self.log.debug(f"nothing to do (table '{table_name}' already exists)")
                return table

        # Create the table and the columns.
        self.log.debug(
            f"creating table '{table_name}' | "
            f"primary_key: '{primary_key}', "
            f"primary_type: '{primary_type}'"
        )
        table = self.db.create_table(table_name, primary_key, primary_type)
        for column_name, column_spec in columns.items():
            column_spec = column_spec or {}  # Ensure that column_spec is not None.

            # If no column_spec is specified, use the default values.
            type = self.map_type(column_spec.get("type", type_hints[column_name]))
            primary_key = column_spec.get("primary_key", False)
            unique = column_spec.get("unique", False)
            nullable = column_spec.get("nullable", True)
            autoincrement = column_spec.get("autoincrement", False)
            default = column_spec.get("default")

            self.log.debug(
                  f"creating column '{column_name}' | "
                  f"type: '{type}', "
                  f"primary_key: {primary_key}, "
                  f"unique: {unique}, "
                  f"nullable: {nullable}, "
                  f"autoincrement: {autoincrement}, "
                  f"default: {default}"
            )
            table.create_column(
                name=column_name,
                type=type,
                primary_key=primary_key,
                unique=unique,
                nullable=nullable,
                autoincrement=autoincrement,
                default=default
            )

        # Create two extra columns, for storing the creation and modification date.
        table.create_column("ts_created", dataset.types.DateTime)
        table.create_column("ts_modified", dataset.types.DateTime)

        # Create the indexes if necessary.
        indexes = yml.get("indexes")
        if indexes:
            for index_name, index_spec in indexes.items():
                # Ensure a 'columns' entry exists.
                columns = index_spec.get("columns")
                if not columns:
                    raise ValueError(f"no columns for index '{index_name}' specified")

                kwargs = {}

                postgresql_using = index_spec.get("postgresql_using")
                if postgresql_using:
                    kwargs["postgresql_using"] = postgresql_using

                postgresql_ops = index_spec.get("postgresql_ops")
                if postgresql_ops:
                    kwargs["postgresql_ops"] = postgresql_ops

                self.log.debug(
                    f"creating index '{index_name}' | "
                    f"columns: {columns}, "
                    f"kwargs: {kwargs}"
                )
                table.create_index(name=index_name, columns=columns, **kwargs)

        return table

    # ----------------------------------------------------------------------------------------------

    def create(self, drop_if_exists: bool = False) -> None:
        """
        TODO
        """
        if self.db.exists() and drop_if_exists:
            self.db.drop()
        return self.db.create()

    def exists(self) -> bool:
        """
        TODO
        """
        return self.db.exists()

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
            raise ValueError(f"no table exists for storing objects of type '{type(object)}'.")

        row = vars(object)
        row["ts_created"] = row["ts_modified"] = datetime.datetime.utcnow()

        self.log.debug(f"insert_one | row: {row}")
        table.insert(row, ensure=False)

    def insert_many(self, objects: list[typing.Any]) -> None:
        """
        TODO
        """
        assert objects is not None, "no objects given"

        # Group the objects per type.
        objects_per_type: dict[type, typing.Any] = {}
        for object in objects:
            if type(object) not in objects_per_type:
                objects_per_type[type(object)] = [object]
            else:
                objects_per_type[type(object)].append(object)

        # Insert the objects in the database.
        for typ, objects in objects_per_type.items():
            table = self.get_table(typ)
            if table is None:
                raise ValueError(f"no table exists for storing objects of type '{typ}'.")

            rows = []
            for object in objects:
                row = vars(object)
                row["ts_created"] = row["ts_modified"] = datetime.datetime.utcnow()
                rows.append(row)

            rows_debug = '\n '.join([str(row) for row in rows])
            self.log.debug(f"insert_many | rows:\n{rows_debug}")
            table.insert_many(rows, ensure=False)

    # ----------------------------------------------------------------------------------------------

    def get_one(self, clazz: type, *_clauses, **kwargs) -> typing.Any:
        """
        TODO
        """
        assert clazz, "no class given"

        # When no table for the given class exists, return nothing.
        table = self.get_table(clazz)
        if not table:
            return None

        # Find the first row matching the specified clauses and arguments.
        row = table.find_one(*_clauses, **kwargs)
        if not row:
            return None

        obj = clazz()
        for key in row:
            setattr(obj, key, row[key])
        return obj

    # ----------------------------------------------------------------------------------------------

    def delete(self, clazz: type, *_clauses, **kwargs):
        """
        TODO
        """
        assert clazz, "no type given"

        # Get the table to delete from.
        table = self.get_table(clazz)
        if not table:
            raise ValueError(f"no table exists for storing objects of type '{clazz}'.")

        # Delete the rows matching the specified clauses and arguments.
        table.delete(*_clauses, **kwargs)

    # ----------------------------------------------------------------------------------------------

    def count(self, clazz: type, *_clauses, **kwargs) -> int:
        """
        TODO
        """
        assert clazz, "no type given"

        # Get the table.
        table = self.get_table(clazz)
        if not table:
            raise ValueError(f"no table exists for storing objects of type '{clazz}'.")

        # Delete the rows matching the specified clauses and arguments.
        return table.count(*_clauses, **kwargs)

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
