#!/usr/bin/env python
from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user='davidemoraschi',
        password='synBKH9KA%D$D8F5',
        account_identifier='qc83116.ca-central-1.aws',
    )
)
try:
    connection = engine.connect().execution_options(autocommit=False)
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()