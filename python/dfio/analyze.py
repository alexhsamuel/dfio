import fixfmt.table

import dfio.db

#-------------------------------------------------------------------------------

def get_num_cols(codename):
    return len(codename)

def print_summary(recs):
    t = fixfmt.table.RowTable()
    for r in recs:
        time = r["time"]["min"]
        items = r["length"] * r["cols"]
        t.append(
            operation   =r["operation"],
            codename    =r["codename"][: 12],  # FIXME
            compression =r["method"].get("comp", None),
            length      =r["length"],
            method      =r["method"]["class"],
            size_ratio  =r.get("file_size", float("nan")) / r["data_size"],
            time        =time,
            bandwidth   =r["data_size"] / time,
            rate        =items / time,
        )

    t.fmts.update(
        size_ratio      =fixfmt.Number(1, 3),
        time            =fixfmt.Number(6, 1, scale="m"),
        rate            =fixfmt.Number(4, 1, scale="M"),
        bandwidth       =fixfmt.Number(4, 1, scale="M"),
    )

    def all_same(n):
        x = t.rows[0][n]
        return all( r[n] == x for r in t.rows )

    for n in t.rows[0]:
        if all_same(n):
            t.fmts[n] = None

    t.print()


#-------------------------------------------------------------------------------

import argparse

def add_filter_args(parser):
    parser.add_argument(
        "--operation", metavar="OP", default=None,
        help="select operation OP")
    parser.add_argument(
        "--codename", metavar="NAME", default=None,
        help="select table schema with CODENAME")
    parser.add_argument(
        "--length", metavar="LEN", type=int, default=None,
        help="select tables of length LEN")
    parser.add_argument(
        "--method", metavar="CLASS", dest="method_class", default=None,
        help="select method CLASS")


def filter_by_args(args, items):
    operation = getattr(args, "operation", None)
    if operation is not None:
        items = ( i for i in items if i["operation"] == operation )

    codename = getattr(args, "codename", None)
    if codename is not None:
        items = ( i for i in items if i["codename"] == codename )

    method_class = getattr(args, "method_class", None)
    if method_class is not None:
        items = ( i for i in items if i["method"]["class"] == method_class )

    length = getattr(args, "length", None)
    if length is not None:
        items = ( i for i in items if i["length"] == length )

    return items

    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path", metavar="DB-PATH", default=dfio.db.DEFAULT_PATH,
        help=f"benchmark results output path [def: {dfio.db.DEFAULT_PATH}]")
    add_filter_args(parser)
    args = parser.parse_args()

    recs = dfio.db.load(path=args.db_path)
    recs = filter_by_args(args, recs)
    print_summary(recs)


if __name__ == "__main__":
    main()
 
