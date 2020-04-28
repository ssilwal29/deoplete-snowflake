import datetime
import operator
import os
import pickle
import re
import subprocess
import tempfile
import threading
from .base import Base
from deoplete.util import getlines, parse_buffer_pattern
from subprocess import CalledProcessError

TABLE_QUERY = """
    select
        table_catalog,
        table_schema,
        table_name,
        column_name,
        column_default,
        data_type
    from {dbname}.information_schema.columns
    where table_schema != 'INFORMATION_SCHEMA'
"""

COLUMN_PATTERN = re.compile(r"(\w+)[.]\w*")
CACHE_PICKLE = os.path.join(tempfile.gettempdir(), "deoplete_snowflake", "snowflake.pickle")


class Source(Base):
    def __init__(self, vim):
        super().__init__(vim)
        self.dup = True
        self.filetypes = ["sql"]
        self.is_volatile = True
        self.mark = "[snowflake]"
        self.min_pattern_length = 1
        self.name = "snowflake"
        self.rank = 350
        self.sorters = ["sorter_rank"]

    def on_init(self, context):
        self.connection = context["vars"].get("deoplete#sources#snowflake#connection")
        self.command = [
            "snowsql",
            "--connection",
            self.connection,
            "--query",
        ]
        self._cache = {"tables": {}, "databases": {}}

    def gather_candidates(self, context):
        self._make_cache(context)
        current_line = context["input"]
        candidates = []
        column_match = COLUMN_PATTERN.search(current_line)
        table_regex = re.compile(
            r"(FROM|from|JOIN|join)\s+((\w+[.](\w+)[.](\w*))|(\w+)[.](\w*)|(\w*))"
        )
        table_match = table_regex.search(current_line)
        if column_match and not table_match:
            # we are doing a column lookup
            # find the table or alias
            table_or_alias = column_match.group(1)
            for table in self._cache["tables"]:
                is_table = table == table_or_alias.upper()
                is_alias = table_or_alias.upper() in self._cache["tables"][table]["aliases"]
                if is_table or is_alias:
                    # append columns
                    for column_data in self._cache["tables"][table]["columns"]:
                        column_name = column_data["name"]
                        column_default = column_data["default"]
                        column_type = column_data["type"]
                        type_string = f"DEFAULT {column_default} DATATYPE {column_type}"
                        candidates.append(
                            {"word": f"{column_name}", "menu": f"[{type_string}]", "kind": "col"}
                        )

        # otherwise, fill the candidates with databases
        if table_match:
            if table_match.groups()[7]:
                for database in self._cache["databases"]:
                    candidates.append({"word": database, "kind": "database"})
            elif table_match.groups()[6]:
                for schema in self._cache["databases"].get(table_match.group(2).split(".")[0], {}):
                    schema_def = {"word": schema, "kind": "schema"}
                    candidates.append(schema_def)
            else:
                # otherwise, fill candidates with all tables and aliases
                dbname_schema = table_match.group(2).split(".")
                dbname = dbname_schema[0]
                schema = dbname_schema[1]
                for table in self._cache["databases"].get(dbname, {}).get(schema, []):
                    candidates.append({"word": table, "kind": "table or view"})
                    for alias in self._cache["tables"][table]["aliases"]:
                        candidates.append({"word": alias, "kind": "alias"})
        candidates.sort(key=operator.itemgetter("word"))
        return candidates

    def _execute_query(self, command):
        try:
            command_results = subprocess.check_output(command, universal_newlines=True).split("\n")
        except CalledProcessError:
            return None

        for row in command_results[4:]:
            if len(row.split("|")) > 5:
                yield row

    def get_complete_position(self, context):
        m = re.search(r"\w*$", context["input"])
        return m.start() if m else -1

    def _cache_db(self):
        temp_dir = tempfile.gettempdir()
        cache_tmp_dir = os.path.join(temp_dir, "deoplete_snowflake")
        os.makedirs(cache_tmp_dir, exist_ok=True)
        if not self._cache["databases"]:
            command = self.command.copy()
            command = command + ["SHOW DATABASES"]
            for row in self._execute_query(command):
                dbname = row.split("|")[2].strip()
                if dbname not in self._cache["databases"]:
                    self._cache["databases"][dbname] = {}

        # populate tables and columns
        if not self._cache["tables"]:
            for dbname in self._cache["databases"]:
                self.vim.command("echo 'Intorspecting {dbname}'".format(dbname=dbname))
                query = TABLE_QUERY.format(dbname=dbname)
                command = self.command.copy()
                command = command + [query]
                for row in self._execute_query(command):
                    row_split = [x.strip() for x in row.split("|")]
                    schema = row_split[2]
                    table = row_split[3]
                    self._cache["databases"][dbname][schema] = self._cache["databases"][dbname].get(
                        schema, []
                    )
                    if table not in self._cache["databases"][dbname][schema]:
                        self._cache["databases"][dbname][schema].append(table)
                    self._cache["tables"][table] = self._cache["tables"].get(table, {})
                    self._cache["tables"][table].setdefault("columns", [])
                    self._cache["tables"][table]["columns"].append(
                        {"name": row_split[4], "default": row_split[5], "type": row_split[6],}
                    )
        with open(CACHE_PICKLE, "wb") as f:
            pickle.dump(self._cache, f, protocol=pickle.HIGHEST_PROTOCOL)

    def _make_cache(self, context):
        # gather databases
        file_exists = os.path.isfile(CACHE_PICKLE)
        file_created_ago = 0
        if file_exists:
            file_created_ago = (
                datetime.datetime.now()
                - datetime.datetime.fromtimestamp(os.stat(CACHE_PICKLE).st_ctime)
            ).days

        if file_exists and file_created_ago < 1:
            self.vim.command("echo 'file already exists'")
            with open(CACHE_PICKLE, "rb") as f:
                self._cache = pickle.load(f)
        else:
            self._cache_db()
            #  th = threading.Thread(target=self._cache_db)
            #  th.start()
            #  if not file_exists:
            #      self.vim.command("echo 'waiting to write the file'")
            #      th.join()
        #
        # gather aliases
        alias_hits = parse_buffer_pattern(
            getlines(self.vim), r"(FROM|JOIN|from|join)\s+(\w+)[.](\w+)([.](\w*))?\s+(\w*)"
        )

        # clear existing aliases
        for table in self._cache["tables"]:
            self._cache["tables"][table]["aliases"] = []

        for alias_hit in alias_hits:
            table = alias_hit[4].upper() or alias_hit[2].upper()
            alias = alias_hit[5].upper()
            if table not in self._cache["tables"]:
                continue

            if alias not in self._cache["tables"][table]["aliases"]:
                self._cache["tables"][table]["aliases"].append(alias)
