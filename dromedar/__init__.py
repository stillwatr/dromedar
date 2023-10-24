import dataset
import datetime
import typing

from dataset import types as db_types
from typing import Annotated, Any

# TODO: Implement column indexes.

# ==================================================================================================


class DbColumn:
    """
    TODO
    """
    type: db_types
    is_primary_key: bool
    unique: bool
    nullable: bool
    autoincrement: bool
    default: Any

    def __init__(
            self,
            type: db_types = None,
            is_primary_key: bool = False,
            unique: bool = False,
            nullable: bool = True,
            autoincrement: bool = False,
            default: Any = None):
        """
        TODO
        """
        self.type = type
        self.is_primary_key = is_primary_key
        self.unique = unique
        self.nullable = nullable
        self.autoincrement = autoincrement
        self.default = default


class DbEntity:
    """
    TODO
    """
    id: Annotated[str, DbColumn(is_primary_key=True)]

    def __init__(self, id: str) -> None:
        self.id = id

# ==================================================================================================


class Database:
    """
    TODO
    """

    def __init__(
            self,
            db_name: str,
            db_user: str = "postgres",
            db_password: str = "postgres",
            db_host: str = "postgres",
            db_port: int = 5432) -> None:
        """
        TODO
        """
        self.db: dataset.Database = dataset.connect(
            url=f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
            ensure_schema=False
        )

    def init_table(self, type: type[DbEntity], force_init: bool = False) -> dataset.Table:
        """
        TODO
        """
        if not type:
            raise ValueError("no class given")

        if not issubclass(type, DbEntity):
            raise ValueError(f"class '{type.__name__}' is not a subclass of 'DbEntity'.")

        # Check if a table for storing entities of the given type already exists.
        # If the user wants to force the init, drop the table. Otherwise, do nothing.
        table: dataset.Table = self.get_table(type)
        if table:
            if force_init:
                table.drop()
            else:
                return table

        # Create the table.
        table = self.db.create_table(type.__name__)

        table_schema = self._compute_table_schema(type)
        for name, column in table_schema.items():
            table.create_column(
                name=name,
                type=self._map_type(column.type),
                primary_key=column.is_primary_key,
                unique=column.unique,
                nullable=column.nullable,
                autoincrement=column.autoincrement,
                default=column.default,
            )

        table.create_column("ts_created", dataset.types.DateTime, nullable=False)
        table.create_column("ts_modified", dataset.types.DateTime, nullable=False)

        return table

    # ----------------------------------------------------------------------------------------------

    def insert_one(self, entity: DbEntity) -> None:
        """
        TODO
        """
        if not entity:
            raise ValueError("no entity given")

        if not isinstance(entity, DbEntity):
            raise ValueError("the object is not an instance of 'DbEntity'")

        table = self.get_table(entity)
        if table is None:
            raise ValueError("no table exists for inserting the entity'")

        row = vars(entity)
        row["ts_created"] = row["ts_modified"] = datetime.datetime.utcnow()

        table.insert(row, ensure=False)

    # ----------------------------------------------------------------------------------------------

    def get_table(self, obj: DbEntity | type[DbEntity]) -> dataset.Table:
        """
        TODO
        """
        table_name = obj.__name__ if isinstance(obj, type) else type(obj).__name__
        return self.db[table_name] if self.db.has_table(table_name) else None

    # ----------------------------------------------------------------------------------------------

    def _compute_table_schema(self, type: type[DbEntity]) -> dict[str, DbColumn]:
        """
        TODO
        """
        schema: dict[str, DbColumn] = {}

        type_hints = typing.get_type_hints(type, include_extras=True)
        for field_name, hint in type_hints.items():
            if typing.get_origin(hint) is not Annotated:
                continue

            col_spec = next((m for m in hint.__metadata__ if isinstance(m, DbColumn)), None)
            if not col_spec:
                continue

            if not col_spec.type:
                col_spec.type = hint.__origin__

            schema[field_name] = col_spec

        return schema

    def _map_type(self, type: type) -> db_types:
        """
        TODO
        """
        if type is str:
            return db_types.String
        if type is bool:
            return db_types.Boolean
        if type is int:
            return db_types.BigInteger
        if type is float:
            return db_types.Float
        if type is datetime:
            return db_types.DateTime
        if type is dict:
            return db_types.JSONB

        return db_types.String
