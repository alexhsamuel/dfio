import fixfmt.table

#-------------------------------------------------------------------------------

def get_num_cols(codename):
    return len(codename)

def print_summary(db, *, operation=None, codename=None, length=None, method_class=None):
    recs = db
    if operation is not None:
        recs = ( r for r in recs if r["operation"] == operation )
    if codename is not None:
        recs = ( r for r in recs if r["codename"] == codename )
    if length is not None:
        recs = ( r for r in recs if r["length"] == length )
    if method_class is not None:
        recs = ( r for r in recs if r["method"]["class"] == method_class )
    
    t = fixfmt.table.RowTable()
    for r in recs:
        time = r["time"]["min"]
        items = r["length"] * get_num_cols(r["codename"])
        t.append(
            operation=r["operation"],
            codename=r["codename"],
            length=r["length"],
            method=r["method"]["class"],
            compression=str(r["method"].get("comp", None)),
            time=time,
            rate=items / time,
            comp_ratio=r.get("compression_ratio", 0),
        )
    t.fmts["time"] = fixfmt.Number(6, 1, scale="m")
    t.fmts["rate"] = fixfmt.Number(6, 1, scale="M")
    t.print()


#-------------------------------------------------------------------------------

import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path", metavar="DB-PATH", default="./dfio-benchmark.json",
        help="benchmark results output path [def: ./dfio-benchmark.json]")
    args = parser.parse_args()



if __name__ == "__main__":
    main()
 
