import sys

from .common.utils import print_log
from .core import download, get_commend_args
from .tools import date_to_height, block_timestamp_cache,aave_risk_param


def main():
    if len(sys.argv) == 1:
        print_log("use parameter -h for help")
        exit(1)
    if len(sys.argv) > 1 and sys.argv[1].endswith(".toml"):
        download(sys.argv[1])
        exit(0)

    args = get_commend_args()
    if args.config is not None:
        download(args.config)
    elif args.tools is not None:
        if args.tools == "date_to_height":
            date_to_height(args)
        elif args.tools == "block_timestamp":
            block_timestamp_cache(args)
        elif args.tools == "aave":
            aave_risk_param(args)
        pass

if __name__ == "__main__":
    main()