import re
from time import sleep

from psycopg2 import OperationalError
from sqlalchemy.sql import text
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import create_engine

import docker


class DdlChecker:
    def __init__(self, ddl: str):
        self._ddl = ddl
        self._no_primary_key_table_names = set()
        self.messages: list[str] = []

    @classmethod
    def _check_mix_upper_lower_case_text(cls, text: str):
        if re.search(r"^(?=.*[a-z])(?=.*[A-Z]).*$", text):
            if not (text[0] == "\"" and text[-1] == "\""):
                return False
        return True

    def _check_create_table_query(self, query: str):
        tokens = query.split()
        table_name = tokens[2]

        if "PRIMARY KEY" not in query:
            self._no_primary_key_table_names.add(table_name)

        if len(table_name) > 63:
            self.messages.append(f"テーブル名の最大長は63文字です ({table_name})")

    def _check_create_index_query(self, query: str, is_unique: bool):
        tokens = query.split()
        index_name = tokens[2]

        if is_unique:
            index_name = tokens[3]

        if not self._check_mix_upper_lower_case_text(text=index_name):
            self.messages.append(f"大文字・小文字混合のインデックス名は\"\"で囲む必要があります ({index_name})")

        if len(index_name) > 63:
            self.messages.append(f"インデックス名の最大長は63文字です ({index_name})")

    def _check_alter_table_add_constraint_query(self, query: str):
        tokens = query.split()

        table_name = tokens[2]
        identifier = tokens[5]

        if not self._check_mix_upper_lower_case_text(text=identifier):
            self.messages.append(f"大文字・小文字混合の識別子は\"\"で囲む必要があります ({identifier})")

        if len(re.sub(r"\"", "", identifier)) > 63:
            self.messages.append(f"識別子の最大長は\"\"を除いて63文字です ({identifier}: {len(identifier)}文字)")

        if tokens[6] == "PRIMARY" and tokens[7] == "KEY":
            if table_name in self._no_primary_key_table_names:
                self._no_primary_key_table_names.remove(table_name)
            else:
                self.messages.append(f"""
                ALTER TABLE {table_name}~はCREATE TABLE {table_name}~の後に記載してください
                またCREATE TABLE {table_name}~の直前の改行が1行になっているかを確認してください
                """)
        elif tokens[6] == "FOREIGN" and tokens[7] == "KEY":
            pass
        elif tokens[6] == "UNIQUE":
            pass

    # ADD CONSTRAINT以外にもADD COLUMNなどがある

    def _check_ddl_syntax(self):
        # TODO: ALTER TABLE xxxx  ADD CONSTRAINT xxxx UNIQUE (xxxx)が検知できない(TABLEのあとが改行)
        for query in re.sub(r"\n{2}", "", self._ddl).split(";"):
            if query.startswith("CREATE TABLE"):
                self._check_create_table_query(query=query)
            elif query.startswith("CREATE INDEX"):
                self._check_create_index_query(query=query, is_unique=False)
            elif query.startswith("CREATE UNIQUE INDEX"):
                self._check_create_index_query(query=query, is_unique=True)
            elif re.match("ALTER TABLE [a-z0-9_]+ ADD CONSTRAINT", query):
                self._check_alter_table_add_constraint_query(query=query)

        for table_name in self._no_primary_key_table_names:
            self.messages.append(f"{table_name}のプライマリキーが定義されていません")

    # SqlAlchemy(psycopg)の例外をもとにエラーメッセージを追加

    def _parse_partition_error_text(self, partition_error_text: str):
        lines = partition_error_text.split()
        table_name = lines[5]
        partition_column_name = lines[8]
        self.messages.append(f"partition byで指定したカラムがプライマリキーに含まれないテーブルがあります ({table_name}.{partition_column_name})")

    # 実際にDDLを実行し、エラーにならないことを確認する。

    def _execute_ddl(self):
        cli = docker.from_env()
        use_image = "postgres:15"

        if not cli.images.list(name=use_image):
            print("[INFO] postgresqlのイメージをダウンロードします")
            cli.images.pull(use_image)

        container = cli.containers.run(
            image=use_image,
            auto_remove=True,
            environment=["POSTGRES_PASSWORD=password"],
            detach=True,
            ports={"5432/tcp": None},
        )
        print("[INFO] postgresqlを起動します")

        bind_port = 0

        while True:
            print(".", end="")
            container.reload()
            match container.attrs:
                case {"NetworkSettings": {"Ports": {"5432/tcp": [*port_settings]}}}:
                    for port_setting in port_settings:
                        match port_setting:
                            case {"HostIp": "0.0.0.0", "HostPort": port}:
                                bind_port = int(port)
                                print(f"\n[INFO] 接続ポート番号を確認しました {bind_port}")
            if bind_port:
                break
            sleep(0.1)

        engine = create_engine(
            f"postgresql+psycopg2://postgres:password@localhost:{bind_port}/postgres",
            echo=False,
            poolclass=NullPool,
        )

        while True:
            print(".", end="")
            try:
                with engine.connect() as conn:
                    print("\n[INFO] postgresqlを起動しました")
                    conn.execute(text(self._ddl))
                    print("\n[INFO] クエリを実行しました")
                    break
            except Exception as e:
                original_error = getattr(e, "orig")
                if isinstance(original_error, OperationalError):
                    pass
                else:
                    original_error_text = str(original_error).lower()

                    partition_error = re.search(
                        r"primary key constraint on table \".+\" lacks column \".+\" which is part of the partition key.",
                        original_error_text
                    )

                    if partition_error:
                        self._parse_partition_error_text(partition_error_text=partition_error.group())
                    else:
                        # 一旦エラー文をそのまま追加する
                        self.messages.append(original_error_text)

                    break
            sleep(0.1)

        container.stop()
        print("\n[INFO] postgresqlを停止しました\n")

    def check_ddl(self) -> list[str]:
        self._check_ddl_syntax()
        self._execute_ddl()
        return self.messages
