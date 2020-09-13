#-------------------------------------------------------------------------------

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

    
