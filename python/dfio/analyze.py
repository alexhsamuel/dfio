import fixfmt.table

import dfio.clui
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
            length      =r["length"],
            method      =r["method"]["class"],
            compression =r["method"].get("comp", None),
            engine      =r["method"].get("engine", ""),
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

    t.set_fmts()
    for n in t.rows[0]:
        if all_same(n):
            val = t.rows[0][n]
            print(f"{n:16s} = {t.fmts[n](val)}")
            t.fmts[n] = None
    print()

    t.print()


#-------------------------------------------------------------------------------

import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path", metavar="DB-PATH", default=dfio.db.DEFAULT_PATH,
        help=f"benchmark results output path [def: {dfio.db.DEFAULT_PATH}]")
    dfio.clui.add_filter_args(parser)
    args = parser.parse_args()

    recs = dfio.db.load(path=args.db_path)
    recs = dfio.clui.filter_by_args(args, recs)
    print_summary(recs)


if __name__ == "__main__":
    main()
 
