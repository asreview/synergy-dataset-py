import argparse

from pyodss._version import __version__
from pyodss.base import Dataset


def main():

    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="Python package for ODSS dataset.",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        help="Dataset name.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Dataset output path.",
    )
    parser.add_argument(
        "-l",
        "--ignore-legal",
        dest="legal",
        help="Ignore legal message.",
        action="store_true",
    )
    # version
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )

    args, _ = parser.parse_known_args()

    if not args.legal:
        user_input = input("I agree with building dataset ([Y]es,[N]o):")
        if user_input.lower() not in ["y", "yes"]:
            return

    d = Dataset(args.dataset)
    result = d.to_frame()
    print(result)

    if args.output:
        result.to_csv(args.output)


if __name__ == "__main__":

    main()
