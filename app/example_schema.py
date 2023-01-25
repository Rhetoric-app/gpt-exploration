from dataclasses import dataclass
from typing import List, Optional

import sqlalchemy as db


@dataclass
class TableSchemaCol:
    name: str
    type: db.types.TypeEngine
    nullable: bool = False
    primary_key: bool = False
    fk: Optional[str] = None


@dataclass
class TableSchema:
    name: str
    desc: str
    cols: List[TableSchemaCol]


# Define all tables
TABLES: List[TableSchema] = [
    TableSchema(
        name='users',
        desc=(
            'This table gives information regarding each user (AKA person, customer) in the app. '
            'In this table, the column "created_at" refers to when a user joined.'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('full_name', db.String(256)),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
    TableSchema(
        name='user_logins',
        desc=(
            'This table gives information regarding the datetime (AKA timestamp, AKA date) of each user login event. '
            'In this table, the column "created_at" refers to the the date and time of a given login.'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('user_id', db.ForeignKey('users.id')),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
    TableSchema(
        name='products',
        desc=(
            'This table gives information regarding each product (AKA item, subscription) for sale in the app. '
            'In this table, the column "amount_cents" refers to the price of an item in cents (USD).'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('amount_cents', db.Integer),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
    TableSchema(
        name='users_purchases',
        desc=(
            'This table gives information regarding each purchase (AKA sale) for each user and product. '
            'Total revenue may be calculated by joining the "products" table and summing "products.amount_cents".'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('user_id', db.ForeignKey('users.id')),
            TableSchemaCol('product_id', db.ForeignKey('products.id')),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
]


def create(metadata_obj: db.MetaData):
    """
    Populate a database schema with some example tables.

    Example usage:
        engine = db.create_engine("sqlite:///:memory:")
        metadata_obj = db.MetaData(bind=engine)
        create(metadata_obj)
    """
    for table in TABLES:
        db.Table(
            table.name,
            metadata_obj,
            *[db.Column(col.name, col.type, nullable=col.nullable, primary_key=col.primary_key) for col in table.cols],
        )
    metadata_obj.create_all()
