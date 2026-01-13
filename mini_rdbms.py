import re
from collections import defaultdict

class Table:
    def __init__(self, name, columns, primary_key=None, unique_keys=None):
        self.name = name
        self.columns = columns  # {col: type}
        self.primary_key = primary_key
        self.unique_keys = unique_keys or []
        self.rows = []
        self.indexes = defaultdict(dict)

        if primary_key:
            self.indexes[primary_key] = {}

        for key in self.unique_keys:
            self.indexes[key] = {}

    def insert(self, row):
        for col in self.columns:
            if col not in row:
                row[col] = None

        # Enforce PK & UNIQUE
        for idx_col in self.indexes:
            val = row[idx_col]
            if val in self.indexes[idx_col]:
                raise ValueError(f"Duplicate value for {idx_col}")

        self.rows.append(row)

        for idx_col in self.indexes:
            self.indexes[idx_col][row[idx_col]] = row

    def select(self, where=None):
        if not where:
            return self.rows

        col, val = where
        if col in self.indexes:
            return [self.indexes[col].get(val)] if val in self.indexes[col] else []
        return [r for r in self.rows if r[col] == val]

    def update(self, updates, where):
        col, val = where
        for row in self.select(where):
            for k, v in updates.items():
                row[k] = v

    def delete(self, where):
        col, val = where
        self.rows = [r for r in self.rows if r[col] != val]


class MiniRDBMS:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, columns, primary_key=None, unique_keys=None):
        self.tables[name] = Table(name, columns, primary_key, unique_keys)

    def insert(self, table, row):
        self.tables[table].insert(row)

    def select(self, table, where=None):
        return self.tables[table].select(where)

    def update(self, table, updates, where):
        self.tables[table].update(updates, where)

    def delete(self, table, where):
        self.tables[table].delete(where)

    def join(self, t1, t2, on1, on2):
        result = []
        for r1 in self.tables[t1].rows:
            for r2 in self.tables[t2].rows:
                if r1[on1] == r2[on2]:
                    result.append({**r1, **r2})
        return result
