"""命令行入口：python -m leasedebug --host ... --port ..."""
from __future__ import annotations

import argparse
import os
import sys

from .webapp import create_server


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="leasedebug", description="分布式租约时序重放调试台")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=5224, help="监听端口")
    parser.add_argument("--db", default=None,
                        help="SQLite 文件（默认 ./.leasedebug.db）")
    args = parser.parse_args(argv)
    db_path = args.db or os.path.join(os.getcwd(), ".leasedebug.db")
    server = create_server(args.host, args.port, db_path)
    print("租约调试台已启动: http://%s:%d" % (args.host, args.port))
    print("数据库: %s" % db_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭……")
    finally:
        server.server_close()
        server.state.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
