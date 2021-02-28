
from aandeg.model import Model
from aandeg.util.config import Config

model = Model(Config().create_connection())

print("""
digraph depend_dot {{
    node [shape=box];
    rankdir="LR";    
    {}
    {}
}}
""".format(
    "\n".join([ "\t{}->{}".format(x[0], x[1]) for x in model.list_table('prod_class_depends')]),
    "\n".join([ "\t{}->{}".format(x[0], x[1]) for x in model.list_table('equip_class_depends')]),
    )
)

