import asynctest

from python_project.database import Database


class MockDatabase(Database):
    def check_database(self, database_version: bytes) -> int:
        self.execute("CREATE TABLE option(key TEXT PRIMARY KEY, value BLOB)")
        self.execute("INSERT INTO option(key, value) VALUES('database_version', '0')")
        self.commit()
        return 0


class TestDatabase(asynctest.TestCase):
    def setUp(self) -> None:
        super(TestDatabase, self).setUp()
        self.database = MockDatabase(u":memory:")

    def test_unloaded(self) -> None:
        """
        Check if an unloaded database returns None for queries.
        """
        self.assertIsNone(self.database.execute("SELECT * FROM option"))

    def test_closed(self) -> None:
        """
        Check if an unloaded database returns None for queries.
        """
        self.database.open()
        self.assertListEqual(
            [(b"database_version", b"0")],
            list(self.database.execute("SELECT * FROM option")),
        )
        self.database.close(True)
        self.assertIsNone(self.database.execute("SELECT * FROM option"))
