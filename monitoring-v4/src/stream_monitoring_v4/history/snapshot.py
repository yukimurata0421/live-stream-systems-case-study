from contextlib import contextmanager
from contextlib import closing


@contextmanager
def read_snapshot(repository):
    """One consistent, read-only snapshot across all streamed tables/queries."""
    with repository.connection(read_only=True) as connection:
        raw = getattr(connection, 'raw', None)
        if raw is not None:
            with raw.transaction():
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                yield connection
        else:
            connection.execute('BEGIN')
            try:
                yield connection
            finally:
                connection.rollback()


@contextmanager
def streamed(connection, sql, params=()):
    stream = getattr(connection, 'stream_cursor', None)
    with (stream(sql, params) if callable(stream) else closing(connection.execute(sql, params))) as cursor:
        yield cursor
