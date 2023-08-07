import sys

from ddl_pre_commit_checker.ddl_checker import DdlChecker


def main():
    if len(sys.argv) < 2:
        print("[ERROR] DDLファイルが指定されていません")
        sys.exit(1)

    try:
        ddl = open(sys.argv[1], mode="r").read()
    except FileNotFoundError:
        print("[ERROR] 指定されたDDLファイルが存在しません")
        sys.exit(1)

    messages = DdlChecker(ddl=ddl).check_ddl()

    if messages:
        print(f"\n{sys.argv[1]}\n")
        for message in messages:
            print(message)

    if messages:
        sys.exit(1)


if __name__ == "__main__":
    main()
